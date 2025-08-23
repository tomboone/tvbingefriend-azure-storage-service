"""Service for interacting with Azure Blob Storage"""
import json
import logging
from typing import Any, List, Dict, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, TableClient, TableEntity, UpdateMode
from azure.storage.blob import ContainerClient, BlobClient
from azure.storage.queue import QueueClient

from .reliability import AzureStorageReliabilityManager


# noinspection PyMethodMayBeStatic,PyUnusedLocal
class StorageService:
    """Service for interacting with Azure Storage"""
    def __init__(self, 
                 connection_string: str,
                 reliability_manager: Optional[AzureStorageReliabilityManager] = None,
                 enable_retry: bool = False) -> None:
        """Initialize the Storage Service

        Args:
            connection_string (str): Connection string for the storage account.
            reliability_manager: Optional reliability manager for retry and health monitoring
            enable_retry: Whether to enable retry functionality (default: False for backward compatibility)
        """
        if connection_string == "UseDevelopmentStorage=true":
            self.connection_string = (
                "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
                "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
            )
        else:
            self.connection_string = connection_string
        
        # Optional reliability features
        self.reliability_manager = reliability_manager
        self.enable_retry = enable_retry
        if self.enable_retry and not self.reliability_manager:
            self.reliability_manager = AzureStorageReliabilityManager(
                connection_string=self.connection_string
            )

    @classmethod
    def create_with_reliability(
            cls,
            connection_string: str,
            max_retry_attempts: int = 3,
            enable_health_monitoring: bool = True
    ) -> 'StorageService':
        """
        Create a StorageService instance with reliability features enabled.

        Args:
            connection_string: Azure Storage connection string
            max_retry_attempts: Maximum number of retry attempts (default: 3)
            enable_health_monitoring: Whether to enable health monitoring (default: True)

        Returns:
            StorageService instance with reliability features enabled
        """
        reliability_manager = AzureStorageReliabilityManager(
            connection_string=connection_string
        )
        
        return cls(
            connection_string=connection_string,
            reliability_manager=reliability_manager,
            enable_retry=True
        )

    def get_reliability_status(self) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive reliability status if reliability features are enabled.

        Returns:
            Dictionary with reliability status, or None if reliability is disabled
        """
        if self.reliability_manager:
            return self.reliability_manager.get_status()
        return None

    def is_healthy(self, service: Optional[str] = None) -> bool:
        """
        Check if Azure Storage services are healthy.

        Args:
            service: Optional specific service to check ('tables', 'queues', 'blobs')

        Returns:
            True if healthy, False if unhealthy or reliability features are disabled
        """
        if self.reliability_manager:
            return self.reliability_manager.is_healthy(service)
        return True  # Assume healthy if no monitoring

    def test_connectivity(self) -> Optional[Dict[str, Any]]:
        """
        Test connectivity to all Azure Storage services.

        Returns:
            Dictionary with connectivity test results, or None if reliability is disabled
        """
        if self.reliability_manager:
            return self.reliability_manager.test_connectivity()
        return None

    def get_queue_service_client(self, queue_name: str) -> QueueClient:
        """Get the queue service client

        Args:
            queue_name (str): Name of the queue
        Returns:
            QueueClient: Client for the queue.
        """
        queue_client = QueueClient.from_connection_string(  # Create queue client
            conn_str=self.connection_string,  # Connection string
            queue_name=queue_name  # Queue name
        )

        try:
            queue_client.create_queue()  # Create queue if it doesn't exist
        except ResourceExistsError:  # If queue already exists, log info
            logging.info(
                msg="StorageService.get_queue_service_client: Queue already exists"
            )

        return queue_client

    def upload_queue_message(self, queue_name: str, message: str | bytes | dict[str, Any]) -> None:
        """Upload a message to the queue

        Args:
            queue_name (str): Name of the queue
            message (str, bytes, dict[str, Any]): Message to upload to queue
        Raises:
            ValueError: If queue name or message is empty.
            Exception: If there is an error uploading the message.
        """
        if not queue_name or not message:  # Check for empty queue name or message
            logging.error(
                msg="StorageService.upload_queue_message: Queue name and message cannot be empty."
            )
            raise ValueError

        logging.debug(
            msg=f"StorageService.upload_queue_message: Attempting to upload message to {queue_name}"
        )

        def upload_operation():
            """Upload message to queue"""
            queue_client = self.get_queue_service_client(queue_name=queue_name)
            
            upload_message = message
            if isinstance(message, dict):
                upload_message = json.dumps(message)

            queue_client.send_message(upload_message)
            
            logging.info(
                msg=f"StorageService.upload_queue_message: Successfully uploaded message to {queue_name}"
            )

        # Apply retry logic if enabled
        if self.reliability_manager and self.enable_retry:
            @self.reliability_manager.reliable_operation(operation_id="queue_upload")
            def reliable_upload():
                """Upload message to queue"""
                return upload_operation()
            
            reliable_upload()
        else:
            try:
                upload_operation()
            except ValueError as e:  # Catch connection string format error from getter
                logging.error(
                    msg=f"StorageService.upload_queue_message: Failed to get queue client for {queue_name}: {e}"
                )
                raise
            except Exception as e:  # Catch any errors
                logging.error(
                    msg=f"StorageService.upload_queue_message: Failed to upload message to {queue_name}: {e}"
                )
                raise

    def get_blob_service_client(self, container_name: str) -> ContainerClient:
        """Get the blob storage container client

        Args:
            container_name (str): Name of the blob storage container

        Returns:
            ContainerClient: Client for the blob storage container.
        """
        container_client = ContainerClient.from_connection_string(  # Create container client
            conn_str=self.connection_string,
            container_name=container_name
        )

        try:
            container_client.create_container()  # Create container if it doesn't exist
        except ResourceExistsError:  # If container already exists, log info
            logging.info("Container already exists")

        return container_client

    def upload_blob_data(
        self, container_name: str, blob_name: str, data: str | bytes | dict | list, overwrite: bool = True
    ) -> None:
        """
        Uploads data to Azure Blob Storage. Serializes Python dicts/lists to JSON.

        Args:
            container_name: The name of the blob container.
            blob_name: The name of the blob.
            data: The data to upload. Dictionaries and lists are automatically serialized to JSON strings.
            overwrite: Whether to overwrite the blob if it already exists.

        Raises:
            ValueError: If container or blob name is invalid.
            azure.core.exceptions.ResourceExistsError: If blob exists and overwrite is False.
            Exception: For other unexpected errors.
        """
        if not container_name or not blob_name:  # Check for empty container name or blob name
            logging.error(
                msg="StorageService.upload_blob_data: Container name and blob name cannot be empty."
            )
            raise ValueError("Container name and blob name cannot be empty.")

        logging.debug(
            msg=f"StorageService.upload_blob_data: Attempting to upload blob to {container_name}/{blob_name} "
                f"(overwrite={overwrite})"
        )
        try:
            blob_client: BlobClient = self.get_blob_service_client(  # Get blob client
                container_name=container_name  # Container name
            ).get_blob_client(
                blob=blob_name  # Blob name
            )
        except ValueError as e:  # Catch connection string format error from getter
            logging.error(
                msg=f"StorageService.upload_blob_data: Failed to get blob client for {container_name}/{blob_name}: {e}"
            )
            raise

        upload_data = data
        # Automatically serialize dicts and lists to a JSON string
        if isinstance(data, (dict, list)):
            upload_data = json.dumps(data)

        try:
            blob_client.upload_blob(  # Upload blob
                data=upload_data,  # Data to upload
                overwrite=overwrite,  # Whether to overwrite existing blob
            )

            logging.info(
                msg=f"StorageService.upload_blob_data: Successfully uploaded blob: {container_name}/{blob_name}"
            )

        except ResourceExistsError as e:  # Catch ResourceExistsError
            if not overwrite:  # If overwrite is False, log warning and re-raise
                logging.warning(
                    msg=f"StorageService.upload_blob_data: Blob {container_name}/{blob_name} already exists and "
                        f"overwrite is False."
                )
                raise
            else:  # If overwrite is True, log error and re-raise
                logging.error(
                    msg=f"StorageService.upload_blob_data: Unexpected ResourceExistsError despite overwrite=True for "
                        f"{container_name}/{blob_name}: {e}"
                )
                raise
        except Exception as e:  # Catch any other errors, log error, and re-raise
            logging.error(
                msg=f"StorageService.upload_blob_data: Failed to upload blob {container_name}/{blob_name}: {e}"
            )
            raise

    def get_table_service_client(self) -> TableServiceClient:
        """Returns an authenticated TableServiceClient instance."""
        try:
            return TableServiceClient.from_connection_string(conn_str=self.connection_string)
        except ValueError as e:
            logging.error(
                msg=f"StorageService.get_table_service_client: Invalid storage connection string format for Table "
                    f"Service: {e}"
            )
            raise ValueError(f"Invalid storage connection string format for Table Service: {e}") from e

    def get_entities(self, table_name: str, filter_query: str | None = None) -> List[Dict[str, Any]]:
        """
        Retrieves entities from a specified Azure Table, with an optional filter.

        Args:
            table_name: The name of the table to query.
            filter_query: An OData filter string to apply to the query.
                          If None, all entities in the table are returned.
                          Example: "PartitionKey eq 'some_key'"

        Returns:
            A list of dictionaries, where each dictionary is an entity.
            Returns an empty list if the table does not exist.

        Raises:
            ValueError: If table_name is invalid.
            azure.core.exceptions.ServiceRequestError: For network or other service issues.
        """
        if not table_name:
            logging.error(msg="StorageService.get_entities: Table name cannot be empty.")
            raise ValueError("Table name cannot be empty.")

        logging.debug(
            msg=f"StorageService.get_entities: Querying entities from table '{table_name}' with filter: "
                f"'{filter_query or 'All'}'"
        )

        def query_operation():
            """Queries Azure table."""
            table_client: TableClient = self.get_table_service_client().get_table_client(table_name=table_name)
            entities: List[Dict[str, Any]]

            if filter_query:
                entities = list(table_client.query_entities(query_filter=filter_query))
            else:
                entities = list(table_client.list_entities())

            logging.info(
                msg=f"StorageService.get_entities: Retrieved {len(entities)} entities from table '{table_name}'."
            )
            return entities

        # Apply retry logic if enabled
        if self.reliability_manager and self.enable_retry:
            @self.reliability_manager.reliable_operation(operation_id="table_query")
            def reliable_query():
                """Queries Azure table."""
                return query_operation()
            
            try:
                return reliable_query()
            except ResourceNotFoundError:
                logging.warning(
                    msg=f"StorageService.get_entities: Table '{table_name}' not found while querying entities. "
                        f"Returning empty list."
                )
                return []
        else:
            try:
                return query_operation()
            except ResourceNotFoundError:
                logging.warning(
                    msg=f"StorageService.get_entities: Table '{table_name}' not found while querying entities. "
                        f"Returning empty list."
                )
                return []
            except Exception as e:
                logging.error(
                    msg=f"StorageService.get_entities: Failed to query entities from table '{table_name}': {e}",
                    exc_info=True
                )
                raise

    def delete_entity(self, table_name: str, partition_key: str, row_key: str) -> None:
        """
        Deletes a specific entity from an Azure Table.
        Does not raise an error if the entity does not exist.

        Args:
            table_name: The name of the target table.
            partition_key: The PartitionKey of the entity to delete.
            row_key: The RowKey of the entity to delete.

        Raises:
            ValueError: If any of the key arguments are invalid.
            azure.core.exceptions.ServiceRequestError: For network or other service issues.
        """
        if not all([table_name, partition_key, row_key]):
            logging.error(msg="StorageService.delete_entity: Table name, partition key, and row key cannot be empty.")
            raise ValueError("Table name, partition key, and row key cannot be empty.")

        logging.debug(
            msg=f"StorageService.delete_entity: Attempting to delete entity from {table_name} "
                f"with PK='{partition_key}' and RK='{row_key}'"
        )
        try:
            table_client: TableClient = self.get_table_service_client().get_table_client(table_name=table_name)
            table_client.delete_entity(partition_key=partition_key, row_key=row_key)
            logging.info(
                msg=f"StorageService.delete_entity: Successfully deleted entity from {table_name} with RowKey "
                    f"'{row_key}'."
            )
        except ResourceNotFoundError:
            logging.warning(
                msg=f"StorageService.delete_entity: Entity not found during deletion, presumed already deleted: "
                f"Table='{table_name}', PK='{partition_key}', RK='{row_key}'"
            )
        except Exception as e:
            logging.error(
                msg=f"StorageService.delete_entity: Failed to delete entity from {table_name} with "
                    f"RowKey '{row_key}': {e}",
                exc_info=True
            )
            raise

    def upsert_entity(self, table_name: str, entity: Dict[str, Any]) -> None:
        """
        Inserts or updates an entity in the specified Azure Table.
        Creates the table if it does not exist.

        Args:
            table_name: The name of the target table.
            entity: A dictionary representing the entity to upsert.
                    Must contain 'PartitionKey' and 'RowKey'.

        Raises:
            ValueError: If table_name is invalid or entity is missing required keys.
            azure.core.exceptions.ServiceRequestError: For network or other service issues.
        """
        if not table_name:
            logging.error(msg="StorageService.upsert_entity: Table name cannot be empty.")
            raise ValueError("Table name cannot be empty.")
        if not all(k in entity for k in ["PartitionKey", "RowKey"]):
            logging.error(msg="Entity must contain 'PartitionKey' and 'RowKey'.")
            raise ValueError("Entity must contain 'PartitionKey' and 'RowKey'.")

        logging.debug(msg=f"StorageService.upsert_entity: Attempting to upsert entity into table '{table_name}'")

        def upsert_operation():
            """Upserts an entity."""
            self.create_table_if_not_exists(table_name)
            table_client: TableClient = self.get_table_service_client().get_table_client(table_name)

            table_client.upsert_entity(entity=entity, mode=UpdateMode.REPLACE)
            logging.info(
                msg=f"StorageService.upsert_entity: Successfully upserted entity with RowKey '{entity.get('RowKey')}' "
                    f"into table '{table_name}'."
            )

        # Apply retry logic if enabled
        if self.reliability_manager and self.enable_retry:
            @self.reliability_manager.reliable_operation(operation_id="table_upsert")
            def reliable_upsert():
                """Upserts an entity."""
                return upsert_operation()
            
            reliable_upsert()
        else:
            try:
                upsert_operation()
            except Exception as e:
                logging.error(
                    msg=f"StorageService.upsert_entity: Failed to upsert entity into table '{table_name}': {e}",
                    exc_info=True
                )
                raise

    def create_table_if_not_exists(self, table_name: str):
        """
        Creates a table if it does not already exist.

        Args:
            table_name: The name of the table to create.
        """
        if not table_name:
            logging.error("StorageService.create_table_if_not_exists: Table name cannot be empty.")
            raise ValueError("Table name cannot be empty.")

        try:
            table_service_client = self.get_table_service_client()
            table_service_client.create_table(table_name=table_name)
            logging.info(f"StorageService.create_table_if_not_exists: Table '{table_name}' created or already exists.")
        except ResourceExistsError:
            logging.debug(f"StorageService.create_table_if_not_exists: Table '{table_name}' already exists.")
        except Exception as e:
            logging.error(f"StorageService.create_table_if_not_exists: Failed to create table '{table_name}': {e}")
            raise

    def delete_table(self, table_name: str):
        """
        Deletes a table. Does not raise an error if the table does not exist.

        Args:
            table_name: The name of the table to delete.
        """
        if not table_name:
            logging.error("StorageService.delete_table: Table name cannot be empty.")
            raise ValueError("Table name cannot be empty.")

        try:
            table_service_client = self.get_table_service_client()
            table_service_client.delete_table(table_name=table_name)
            logging.info(f"StorageService.delete_table: Successfully deleted table '{table_name}'.")
        except ResourceNotFoundError:
            logging.warning(f"StorageService.delete_table: Table '{table_name}' not found, presumed already deleted.")
        except Exception as e:
            logging.error(f"StorageService.delete_table: Failed to delete table '{table_name}': {e}")
            raise

    def delete_entities_batch(self, table_name: str, entities: List[Dict[str, Any]]) -> None:
        """
        Deletes a list of entities from a table in batches of 100.

        Args:
            table_name: The name of the target table.
            entities: A list of entity dictionaries to delete. Each must have PartitionKey and RowKey.
        """
        if not entities:
            return

        table_client = self.get_table_service_client().get_table_client(table_name)
        for i in range(0, len(entities), 100):
            batch = entities[i:i + 100]
            operations = [
                ("delete", TableEntity(PartitionKey=e["PartitionKey"], RowKey=e["RowKey"]))
                for e in batch
            ]
            try:
                table_client.submit_transaction(operations=operations)
                logging.info(f"Successfully deleted batch of {len(operations)} entities from '{table_name}'.")
            except ResourceNotFoundError:
                logging.warning(
                    f"StorageService.delete_entities_batch: Table '{table_name}' not found while deleting a batch, "
                    f"presumed already deleted. Halting further batches for this table."
                )
                break
            except Exception as e:
                logging.error(f"Error deleting batch from table '{table_name}': {e}")
                raise
