"""Enhanced retry handling for Azure Storage operations."""

import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, Any, Type, Dict, TypedDict
from functools import wraps
from collections import defaultdict
from logging import Logger, getLogger

from azure.core.exceptions import (
    AzureError, 
    ServiceRequestError, 
    HttpResponseError,
    ResourceNotFoundError,
    ResourceExistsError
)


class BackoffState(TypedDict):
    """Backoff state for Azure Storage operations"""
    consecutive_failures: int
    last_failure_time: Optional[datetime]
    backoff_until: Optional[datetime]


# noinspection PyMethodMayBeStatic
class AzureStorageRetryHandler:
    """Enhanced retry handler specifically for Azure Storage operations.
    
    Handles Azure Storage specific errors with exponential backoff and
    intelligent retry logic.
    """
    
    def __init__(self, 
                 max_attempts: int = 3,
                 base_delay_seconds: float = 0.5,
                 max_delay_seconds: float = 30.0,
                 logger: Optional[Logger] = None):
        """Initialize the Azure Storage retry handler.
        
        Args:
            max_attempts: Maximum retry attempts
            base_delay_seconds: Base delay for exponential backoff
            max_delay_seconds: Maximum delay between retries
            logger: Optional logger instance
        """
        self.max_attempts = max_attempts
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.logger = logger or getLogger(__name__)
        
        # Track backoff state for different operation types
        self.backoff_state: Dict[str, BackoffState] = defaultdict(lambda: BackoffState(
            consecutive_failures=0,
            last_failure_time=None,
            backoff_until=None
        ))
    
    def is_throttling_error(self, exception: Exception) -> bool:
        """Check if an exception indicates Azure Storage throttling.
        
        Args:
            exception: The exception to check
            
        Returns:
            True if this is a throttling error
        """
        if isinstance(exception, HttpResponseError):
            status_code = getattr(exception, 'status_code', None)
            if status_code == 503:  # Service Unavailable - typical throttling response
                return True
            if status_code == 429:  # Too Many Requests
                return True
        
        error_message = str(exception).lower()
        throttling_indicators = [
            'server busy',
            'throttled',
            'rate limit',
            'too many requests',
            '503',
            '429'
        ]
        
        return any(indicator in error_message for indicator in throttling_indicators)
    
    def is_retriable_error(self, exception: Exception) -> bool:
        """Check if an exception should be retried.
        
        Args:
            exception: The exception to check
            
        Returns:
            True if the error should be retried
        """
        # Don't retry business logic errors
        if isinstance(exception, (ValueError, TypeError)):
            return False
        
        # Don't retry resource not found errors (these are often expected)
        if isinstance(exception, ResourceNotFoundError):
            return False
        
        # Don't retry resource exists errors when they're expected
        if isinstance(exception, ResourceExistsError):
            return False
        
        # Retry throttling errors
        if self.is_throttling_error(exception):
            return True
        
        # Retry general Azure service errors
        if isinstance(exception, (ServiceRequestError, HttpResponseError)):
            if hasattr(exception, 'status_code'):
                status_code = exception.status_code
                # Retry on server errors, but not client errors (4xx except 429)
                return status_code >= 500 or status_code == 429
            return True
        
        # Retry general Azure errors (network issues, etc.)
        if isinstance(exception, AzureError):
            return True
        
        # Retry connection and timeout errors
        error_message = str(exception).lower()
        network_error_indicators = [
            'connection',
            'timeout',
            'network',
            'dns',
            'socket'
        ]
        
        return any(indicator in error_message for indicator in network_error_indicators)
    
    def calculate_backoff_delay(self, attempt: int, is_throttling: bool = False) -> float:
        """Calculate backoff delay for retry attempt.
        
        Args:
            attempt: Current attempt number (1-based)
            is_throttling: Whether this is due to throttling
            
        Returns:
            Delay in seconds
        """
        if is_throttling:
            # Longer delay for throttling errors
            return min(10.0, self.base_delay_seconds * 20)
        
        # Exponential backoff for other errors
        delay = self.base_delay_seconds * (2 ** (attempt - 1))
        return min(delay, self.max_delay_seconds)
    
    def handle_failure(self, operation_id: str, exception: Exception) -> None:
        """Handle a failed operation for backoff tracking.
        
        Args:
            operation_id: Identifier for the operation type
            exception: The exception that occurred
        """
        backoff_state = self.backoff_state[operation_id]
        backoff_state['consecutive_failures'] += 1
        backoff_state['last_failure_time'] = datetime.now(timezone.utc)
        
        is_throttling = self.is_throttling_error(exception)
        backoff_seconds = self.calculate_backoff_delay(
            backoff_state['consecutive_failures'], 
            is_throttling
        )
        
        backoff_state['backoff_until'] = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
        
        self.logger.warning(
            f"Azure Storage failure #{backoff_state['consecutive_failures']} for {operation_id}. "
            f"Backing off for {backoff_seconds:.1f} seconds. Error: {exception}"
        )
    
    def handle_success(self, operation_id: str) -> None:
        """Handle a successful operation.
        
        Args:
            operation_id: Identifier for the operation type
        """
        if operation_id in self.backoff_state:
            self.backoff_state[operation_id] = BackoffState(
                consecutive_failures=0,
                last_failure_time=None,
                backoff_until=None
            )
    
    def check_backoff(self, operation_id: str) -> None:
        """Check if operation should wait due to backoff.
        
        Args:
            operation_id: Identifier for the operation type
        """
        backoff_state = self.backoff_state[operation_id]
        if backoff_state['backoff_until'] is not None and datetime.now(timezone.utc) < backoff_state['backoff_until']:
            wait_time = (backoff_state['backoff_until'] - datetime.now(timezone.utc)).total_seconds()
            self.logger.info(f"In backoff period for {operation_id}. Waiting {wait_time:.1f} seconds.")
            time.sleep(wait_time)
    
    def with_retry(self, 
                   operation_id: str = "azure_storage", 
                   max_attempts: Optional[int] = None,
                   exception_types: tuple[Type[Exception], ...] = (Exception,)) -> Callable:
        """Decorator for adding retry logic to Azure Storage operations.
        
        Args:
            operation_id: Identifier for the operation (for backoff tracking)
            max_attempts: Override default max attempts
            exception_types: Tuple of exception types to catch and retry
            
        Returns:
            Decorated function with retry logic
        """
        def decorator(func: Callable) -> Callable:
            """Decorator for adding retry logic to Azure Storage operations."""
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                """Wrapper function."""
                attempts = max_attempts or self.max_attempts
                last_exception = None
                
                # Check if we're in a backoff period
                self.check_backoff(operation_id)
                
                for attempt in range(1, attempts + 1):
                    try:
                        result = func(*args, **kwargs)
                        # Success - reset backoff state
                        self.handle_success(operation_id)
                        return result
                        
                    except exception_types as e:
                        last_exception = e
                        
                        # Only retry if it's a retriable error
                        if not self.is_retriable_error(e):
                            self.logger.debug(f"Non-retriable error for {operation_id}: {e}")
                            raise
                        
                        if attempt < attempts:
                            is_throttling = self.is_throttling_error(e)
                            delay = self.calculate_backoff_delay(attempt, is_throttling)
                            
                            self.logger.warning(
                                f"Azure Storage attempt {attempt}/{attempts} failed for {operation_id}. "
                                f"Retrying in {delay:.1f}s. Error: {e}"
                            )
                            
                            time.sleep(delay)
                        else:
                            # All attempts failed - update backoff state
                            self.handle_failure(operation_id, e)
                            self.logger.error(
                                f"All {attempts} attempts failed for Azure Storage {operation_id}. Error: {e}"
                            )
                
                # All attempts failed
                raise last_exception
                
            return wrapper
        return decorator
    
    def get_status(self, operation_id: str = "azure_storage") -> dict[str, Any]:
        """Get retry status for an operation.
        
        Args:
            operation_id: Operation identifier
            
        Returns:
            Dictionary with retry status information
        """
        backoff_state = self.backoff_state[operation_id]
        now = datetime.now(timezone.utc)
        
        return {
            'operation_id': operation_id,
            'consecutive_failures': backoff_state['consecutive_failures'],
            'in_backoff_period': (
                backoff_state['backoff_until'] is not None and now < backoff_state['backoff_until']
            ),
            'backoff_until': (
                backoff_state['backoff_until'].isoformat() 
                if backoff_state['backoff_until'] is not None else None
            ),
            'last_failure_time': (
                backoff_state['last_failure_time'].isoformat() 
                if backoff_state['last_failure_time'] is not None else None
            ),
            'max_attempts': self.max_attempts,
            'current_time': now.isoformat()
        }
