"""Combined reliability features for Azure Storage - retry handling and health monitoring."""

from typing import Optional, Callable, Any, Dict
from logging import Logger, getLogger
from functools import wraps

from .retry_handler import AzureStorageRetryHandler
from .health_monitor import AzureStorageHealthMonitor


class AzureStorageReliabilityManager:
    """Combines retry handling and health monitoring for robust Azure Storage usage.
    
    This class provides a unified interface for both retry logic and health monitoring,
    making it easy to add reliability features to any Azure Storage client.
    """
    
    def __init__(self,
                 connection_string: str,
                 retry_handler: Optional[AzureStorageRetryHandler] = None,
                 health_monitor: Optional[AzureStorageHealthMonitor] = None,
                 logger: Optional[Logger] = None):
        """Initialize the reliability manager.
        
        Args:
            connection_string: Azure Storage connection string
            retry_handler: Optional retry handler instance (creates default if None)
            health_monitor: Optional health monitor instance (creates default if None)  
            logger: Optional logger instance
        """
        self.connection_string = connection_string
        self.logger = logger or getLogger(__name__)
        self.retry_handler = retry_handler or AzureStorageRetryHandler(logger=self.logger)
        self.health_monitor = health_monitor or AzureStorageHealthMonitor(
            connection_string=connection_string,
            logger=self.logger
        )
    
    def reliable_operation(
            self,
            operation_id: str = "azure_storage",
            max_attempts: Optional[int] = None
    ) -> Callable:
        """Decorator that adds retry logic to Azure Storage operations.
        
        Args:
            operation_id: Identifier for the operation (for tracking)
            max_attempts: Override default retry attempts
            
        Returns:
            Decorator function that adds retry logic
        """
        def decorator(func: Callable) -> Callable:
            """Apply retry logic"""
            retry_decorated = self.retry_handler.with_retry(
                operation_id=operation_id,
                max_attempts=max_attempts
            )(func)
            
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                """Wrapper function."""
                return retry_decorated(*args, **kwargs)
            
            return wrapper
        return decorator
    
    def get_status(self, operation_id: str = "azure_storage") -> Dict[str, Any]:
        """Get comprehensive status of reliability features.
        
        Args:
            operation_id: Operation identifier
            
        Returns:
            Dictionary with status of both retry handling and health monitoring
        """
        return {
            'retry_handling': self.retry_handler.get_status(operation_id),
            'health_status': self.health_monitor.get_comprehensive_health_status(),
            'performance_metrics': self.health_monitor.get_performance_metrics(),
            'operation_id': operation_id
        }
    
    def is_healthy(self, service: Optional[str] = None) -> bool:
        """Check if Azure Storage services are in a healthy state.
        
        Args:
            service: Optional specific service to check
            
        Returns:
            True if healthy (can make requests without issues)
        """
        try:
            # Check health monitoring
            health_ok = self.health_monitor.is_healthy(service)
            
            # Check if we're not in excessive backoff
            retry_status = self.retry_handler.get_status()
            not_failing_excessively = retry_status['consecutive_failures'] < 5
            
            return health_ok and not_failing_excessively
        except Exception as e:
            self.logger.error(f"Failed to check health status: {e}")
            return False
    
    def test_connectivity(self) -> Dict[str, Any]:
        """Test connectivity to all Azure Storage services.
        
        Returns:
            Dictionary with connectivity test results
        """
        return self.health_monitor.get_comprehensive_health_status(use_cache=False)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for Azure Storage operations.
        
        Returns:
            Dictionary with performance metrics
        """
        return self.health_monitor.get_performance_metrics()
    
    def clear_health_cache(self) -> None:
        """Clear the health monitoring cache to force fresh checks."""
        self.health_monitor.clear_cache()
