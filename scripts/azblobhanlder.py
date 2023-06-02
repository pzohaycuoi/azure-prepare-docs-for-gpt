import os
import re
import logging
from common import logger_config, log_function_call
from azure.core.credentials import AzureSasCredential
from azure.storage.blob import BlobServiceClient


logger_config()
logger = logging.getLogger("file")

class BlobHandler():
    def __init__(self, blob_url: str):
        self.blob_url = blob_url
        self.process_blob_url()
    
    # TODO: implement checking for blob_url later
    # def _validate_url(self):
    
    @log_function_call
    def _split_path(self, file_path):
        """
        Splits the given file path into its directory path and filename.

        Args:
            file_path (str): The file path to be split.

        Returns:
            tuple: A tuple containing the directory path and the filename.
        """
        if file_path != '': splitted_path = os.path.split(file_path)
        else: splitted_path = file_path
        return splitted_path
    
    @log_function_call
    def process_blob_url(self):
        """
        Processes the given blob URL to extract relevant information.

        This method extracts the account URL, storage account name, container name,
        folder path, and blob name from the given blob URL.

        Returns:
            tuple: A tuple containing the account URL, storage account name, container name,
            folder path, and blob name.
        """
        account_url = re.findall(r"(https://\S+windows.net)", self.blob_url)[0]
        regex_compile = re.compile(r"https://(.+?).blob.core.windows.net/(.*)")
        storageaccount_name = regex_compile.search(self.blob_url).group(1)
        file_path = regex_compile.search(self.blob_url).group(2)
        folder_and_file = self._split_path(file_path)
        if folder_and_file == "":
            blob_path = ""
            container_name = ""
            folder_path = ""
            blob_name = ""
        else:
            container_name = folder_and_file[0].split("/")[0]
            blob_path = folder_and_file[0].replace(container_name, "")
            folder_path = blob_path.replace(container_name, "")
            blob_name = folder_and_file[1]
        
        # TODO: get container name
        self.blob_info = {
            "storageaccount_name": storageaccount_name,
            "container_name": container_name,
            "blob_path": blob_path,
            "blob_name": blob_name,
            "account_url": account_url,
            "file_path": file_path,
            "folder_path": folder_path
        }

        return self.blob_info

    @log_function_call
    def _ensure_blob_info(self):
        """
        Ensures the existence of the blob_info attribute.

        This method checks if the blob_info attribute exists. If it doesn't, it calls the
        process_blob_url method to generate and set the blob_info attribute. If the blob_info
        attribute is still not identified after the call, an error is logged, and an AttributeError
        is raised.

        Returns:
            dict: The blob_info attribute.

        Raises:
            AttributeError: If the blob_info attribute is not identified.
        """
        if not hasattr(self, "blob_info"):
            self.process_blob_url()
            if not hasattr(self, "blob_info"):
                logger.error("blob_info attribute is not identifed")
                raise AttributeError("blob_info attribute is not identifed")

        if self.blob_info == "" or self.blob_info == None:
            logger.error("blob_info attribute is not identifed")
            raise AttributeError("blob_info attribute is not identifed")
        
        return self.blob_info

    # TODO: implement this using managed identity, after the function have been deployed to Azure
    # def _check_for_credential(self):
    
    @log_function_call
    def create_blob_service_client(self):
        """
        Creates and returns a BlobServiceClient object for accessing Azure Blob Storage.

        This method creates a BlobServiceClient object using the account URL and Azure SAS token
        retrieved from the blob_info dictionary. The Azure SAS token is obtained from the
        environment variable AZURE_STORAGE_SASTOKEN.

        Returns:
            BlobServiceClient: The created BlobServiceClient object.
        """
        # TOOD: implement _check_for_credential into this function and handling error
        self._ensure_blob_info()
        self.blob_service_client = BlobServiceClient(account_url=self.blob_info["account_url"], credential=AzureSasCredential(signature=os.getenv("AZURE_STORAGE_SASTOKEN")))
    
    @log_function_call
    def _ensure_blob_service_client(self):
        """
        Ensures the existence of the blob_service_client attribute.

        This method checks if the blob_service_client attribute exists. If it doesn't, it calls
        the create_blob_client method to generate and set the blob_service_client attribute.
        If the blob_service_client attribute is still not existent after the call, an error is
        logged, and an AttributeError is raised.

        Returns:
            BlobServiceClient: The blob_service_client attribute.

        Raises:
            AttributeError: If the blob_service_client attribute is not existent.
        """
        if not hasattr(self, "blob_service_client"):
            self.create_blob_client()

        if not hasattr(self, "blob_service_client") or self.blob_service_client is None:
            logger.error("Blob service client does not exist")
            raise AttributeError("Blob service_client does not exist")

        return self.blob_service_client

    @log_function_call
    def create_container_client(self, container: str = None):
        """
        Creates and returns a ContainerClient object for the specified container.

        This method creates a ContainerClient object for the specified container using the
        BlobServiceClient object. If no container name is provided, it retrieves the container
        name from the blob_info dictionary. If the container is not defined, an error is logged
        and a ValueError is raised.

        Args:
            container (str, optional): The name of the container. Defaults to an empty string.

        Returns:
            ContainerClient: The created ContainerClient object.

        Raises:
            ValueError: If the container is not defined.
        """
        # TODO: handling error
        self._ensure_blob_service_client()
        if container == None:
            self.blob_container_client = self.blob_service_client.get_container_client(self.blob_info["container_name"])
        else: self.blob_container_client = self.blob_service_client.get_container_client(container)
    
    @classmethod
    @log_function_call
    def upload_file(self, blob_name, filepath: str, container: str = None):
        """
        Uploads a file to the specified container in Azure Blob Storage.

        This method uploads a file located at the given filepath to the specified container
        using the ContainerClient object. If the container does not exist, it is created.
        The file is uploaded with the specified blob name, overwriting any existing blob with
        the same name.

        Args:
            blob_name (str): The name of the blob to be uploaded.
            filepath (str): The path to the file to be uploaded.
            container (str, optional): The name of the container. Defaults to an empty string.
        """
        # TODO: this function has to return state
        if container != None:
            self.create_container_client(container)
            if not self.blob_container_client.exists():
                self.blob_container_client.create_container()
            # TODO: handling error, double check on this code, this will get IO stream ouput from formrecoginzer
            with open(filepath, "rb") as data:
                self.blob_container_client.upload_blob(blob_name, data, overwrite=True)
        else:
            logger.error("container is not defined")
            raise ValueError("container is not defined")

    @classmethod
    @log_function_call
    def remove_blobs(self, blob_path: str, container: str = ""):
        """
        Removes a blob from the specified container in Azure Blob Storage.

        This method removes the blob at the given file path from the specified container
        using the ContainerClient object. If the container does not exist, an error is logged
        and the blob is not deleted.

        Args:
            file_path (str): The path of the blob to be removed.
            container (str, optional): The name of the container. Defaults to an empty string.
        """
        self.create_container_client(container)
        self.blob_container_client
        if self.blob_container_client.exists():
            self.blob_container_client.delete_blob(blob_path)
    
    @log_function_call
    def create_blob_client(self, file_path: str = None, container: str = None):
        """
        Creates and returns a BlobClient object for the specified blob in Azure Blob Storage.

        This method creates a BlobClient object for the specified blob using the existing
        BlobServiceClient object. If the file_path or container parameters are not provided,
        they are retrieved from the blob_info dictionary. If the blob path or container name
        is not defined, an error is logged and a ValueError is raised.

        Args:
            file_path (str, optional): The path of the blob. Defaults to None.
            container (str, optional): The name of the container. Defaults to None.

        Returns:
            BlobClient: The created BlobClient object.

        Raises:
            ValueError: If the blob path or container name is not defined.
        """
        self._ensure_blob_service_client()
        if file_path == None:
            file_path = self.blob_info["file_path"]
            if file_path == None or file_path == "":
                logger.error("blob_path is not defined")
                raise ValueError("blob_path is not defined")
            
        if container == None:
            container = self.blob_container_client
            if container == None or container == "":
                container = self.blob_info["container_name"]
                if container == None or container == "":
                    logger.error("container_name is not defined")
                    raise ValueError("container_name is not defined")
        
        self.blob_client = self.blob_service_client.get_blob_client(container=container, blob=file_path)
        return self.blob_client
