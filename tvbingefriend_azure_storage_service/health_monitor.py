"""Health monitoring functionality for Azure Storage operations."""

import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from logging import Logger, getLogger

from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient


# noinspection PyUnusedLocal
class AzureStorageHealthMonitor:
    """Health monitoring for Azure Storage services.
    
    Provides health checks and connectivity testing for Azure Storage
    operations including tables, queues, and blobs.
    """
    
    def __init__(self, 
                 connection_string: str,
                 logger: Optional[Logger] = None):
        """Initialize the health monitor.
        
        Args:
            connection_string: Azure Storage connection string
            logger: Optional logger instance
        """
        self.connection_string = connection_string
        self.logger = logger or getLogger(__name__)
        
        # Cache for health check results
        self._last_health_check = None
        self._health_cache_ttl = timedelta(minutes=5)
        self._cached_health_status = None
    
    def test_table_connectivity(self, timeout: float = 10.0) -> Dict[str, Any]:
        """Test connectivity to Azure Table Storage.
        
        Args:
            timeout: Timeout in seconds for the test
            
        Returns:
            Dictionary with connectivity test results
        """
        start_time = time.time()
        result = {
            'service': 'tables',
            'healthy': False,
            'response_time_ms': 0.0,
            'error': None,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        try:
            table_service = TableServiceClient.from_connection_string(self.connection_string)
            
            # Try to list tables (lightweight operation)
            list(table_service.list_tables(results_per_page=1))
            
            result['healthy'] = True
            self.logger.debug("Table Storage connectivity test passed")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.warning(f"Table Storage connectivity test failed: {e}")
        
        finally:
            result['response_time_ms'] = round((time.time() - start_time) * 1000, 2)
        
        return result
    
    def test_queue_connectivity(self, timeout: float = 10.0) -> Dict[str, Any]:
        """Test connectivity to Azure Queue Storage.
        
        Args:
            timeout: Timeout in seconds for the test
            
        Returns:
            Dictionary with connectivity test results
        """
        start_time = time.time()
        result = {
            'service': 'queues',
            'healthy': False,
            'response_time_ms': 0.0,
            'error': None,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        try:
            queue_service = QueueServiceClient.from_connection_string(self.connection_string)
            
            # Try to list queues (lightweight operation)
            list(queue_service.list_queues(results_per_page=1))
            
            result['healthy'] = True
            self.logger.debug("Queue Storage connectivity test passed")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.warning(f"Queue Storage connectivity test failed: {e}")
        
        finally:
            result['response_time_ms'] = round((time.time() - start_time) * 1000, 2)
        
        return result
    
    def test_blob_connectivity(self, timeout: float = 10.0) -> Dict[str, Any]:
        """Test connectivity to Azure Blob Storage.
        
        Args:
            timeout: Timeout in seconds for the test
            
        Returns:
            Dictionary with connectivity test results
        """
        start_time = time.time()
        result = {
            'service': 'blobs',
            'healthy': False,
            'response_time_ms': 0.0,
            'error': None,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        try:
            blob_service = BlobServiceClient.from_connection_string(self.connection_string)
            
            # Try to list containers (lightweight operation)
            list(blob_service.list_containers(results_per_page=1))
            
            result['healthy'] = True
            self.logger.debug("Blob Storage connectivity test passed")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.warning(f"Blob Storage connectivity test failed: {e}")
        
        finally:
            result['response_time_ms'] = round((time.time() - start_time) * 1000, 2)
        
        return result
    
    def get_comprehensive_health_status(self, use_cache: bool = True) -> Dict[str, Any]:
        """Get comprehensive health status for all Azure Storage services.
        
        Args:
            use_cache: Whether to use cached results if available
            
        Returns:
            Dictionary with comprehensive health status
        """
        now = datetime.now(timezone.utc)
        
        # Use cached result if it's still valid
        if (
                use_cache and
                self._last_health_check and
                self._cached_health_status and
                now - self._last_health_check < self._health_cache_ttl
        ):
            
            return self._cached_health_status
        
        # Run health checks for all services
        table_health = self.test_table_connectivity()
        queue_health = self.test_queue_connectivity()
        blob_health = self.test_blob_connectivity()
        
        # Calculate overall health
        all_services = [table_health, queue_health, blob_health]
        healthy_services = [s for s in all_services if s['healthy']]
        
        overall_healthy = len(healthy_services) == len(all_services)
        avg_response_time = sum(s['response_time_ms'] for s in all_services) / len(all_services)
        
        health_status = {
            'overall_healthy': overall_healthy,
            'timestamp': now.isoformat(),
            'services': {
                'tables': table_health,
                'queues': queue_health,
                'blobs': blob_health
            },
            'summary': {
                'healthy_services': len(healthy_services),
                'total_services': len(all_services),
                'average_response_time_ms': round(avg_response_time, 2),
                'health_score': round((len(healthy_services) / len(all_services)) * 100, 1)
            }
        }
        
        # Cache the result
        self._cached_health_status = health_status
        self._last_health_check = now
        
        return health_status
    
    def is_healthy(self, service: Optional[str] = None) -> bool:
        """Check if Azure Storage services are healthy.
        
        Args:
            service: Optional specific service to check ('tables', 'queues', 'blobs')
                    If None, checks overall health
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            health_status = self.get_comprehensive_health_status()
            
            if service:
                return health_status['services'].get(service, {}).get('healthy', False)
            else:
                return health_status['overall_healthy']
                
        except Exception as e:
            self.logger.error(f"Failed to check health status: {e}")
            return False
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for Azure Storage operations.
        
        Returns:
            Dictionary with performance metrics
        """
        health_status = self.get_comprehensive_health_status()
        
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'response_times': {
                service: data['response_time_ms'] 
                for service, data in health_status['services'].items()
            },
            'average_response_time_ms': health_status['summary']['average_response_time_ms'],
            'health_score': health_status['summary']['health_score'],
            'services_status': {
                service: 'healthy' if data['healthy'] else 'unhealthy'
                for service, data in health_status['services'].items()
            }
        }
    
    def clear_cache(self) -> None:
        """Clear the health status cache."""
        self._cached_health_status = None
        self._last_health_check = None
