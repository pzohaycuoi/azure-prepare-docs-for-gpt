import os
import re
import logging
from azure.core.credentials import AzureSasCredential
from azure.storage.blob import BlobServiceClient


class BlobHandler():
    def __init__(self, blob_url: str):
        self.blob_url = blob_url
        self.process_blob_url()
    
    # TODO: implement checking for blob_url later
    # def _validate_url(self):
    
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
            blob_path = folder_and_file[0]
            container_name = folder_and_file[0].split("/")[0]
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
    
    # TODO: implement this using managed identity, after the function have been deployed to Azure
    # def _check_for_credential(self):
            
    def create_blob_service(self):
        """
        Creates and returns a BlobServiceClient object for accessing Azure Blob Storage.

        This method creates a BlobServiceClient object using the account URL and Azure SAS token
        retrieved from the blob_info dictionary. The Azure SAS token is obtained from the
        environment variable AZURE_STORAGE_SASTOKEN.

        Returns:
            BlobServiceClient: The created BlobServiceClient object.
        """
        # TOOD: implement _check_for_credential into this function and handling error
        self.blob_service = BlobServiceClient(account_url=self.blob_info["account_url"], credential=AzureSasCredential(signature=os.getenv("AZURE_STORAGE_SASTOKEN")))
        return self.blob_service
    
    def _ensure_blob_service(self):
        """
        Ensures that a valid BlobServiceClient object exists.

        This method checks if the blob_service attribute is empty or None. If it is, an error
        is logged, and an AttributeError is raised indicating that the blob service does not exist.

        Raises:
            AttributeError: If the blob service does not exist.
        """
        # TODO: handling error
        if self.blob_service == "" or self.blob_service == None:
            logging.error("Blob service is not exist")
            raise AttributeError("Blob service is not exist")
    
    def create_container_client(self, container: str = ""):
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
        self.create_blob_service()
        self._ensure_blob_service()
        if container == "":
            if self.blob_info["container_name"] == "":
                logging.error("Container is not defined")
                raise ValueError("Container is not defined")
            else:
                self.blob_container = self.blob_service.get_container_client(self.blob_info["container_name"])
        else: self.blob_container = self.blob_service.get_container_client(container)
            
        return self.blob_container
    
    @classmethod
    def upload_file(self, blob_name, filepath: str, container: str = ""):
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
        self.create_container_client(container)
        if not self.blob_container.exists():
            self.blob_container.create_container()
        # TODO: handling error
        with open(filepath, "rb") as data:
            self.blob_container.upload_blob(blob_name, data, overwrite=True)

    @classmethod
    def remove_blobs(self, file_path: str, container: str = ""):
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
        self.blob_container
        if self.blob_container.exists():
            self.blob_container.delete_blob(file_path)
            # TODO: the logic below will belong in other function, keep this for later use
            # prefix = os.path.splitext(os.path.basename(file_path))[0]
            # blobs = filter(lambda b: re.match(f"{prefix}-\d+\.word", b), self.blob_container.list_blob_names(name_starts_with=os.path.splitext(os.path.basename(prefix))[0]))
            # for b in blobs:
            #     self.blob_container.delete_blob(b)
