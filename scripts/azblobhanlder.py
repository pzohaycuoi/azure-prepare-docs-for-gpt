import os
import re
import logging
from common import logger_config, log_function_call
from azure.identity import ChainedTokenCredential
from azure.core.credentials import AzureSasCredential
from azure.storage.blob import BlobServiceClient


logger_config()
logger = logging.getLogger("file")

class BlobHandler():
    def __init__(self, blob_url: str, credential):
        self.blob_url = blob_url
        self.credential = credential
        self.process_blob_url()
        self.create_blob_service_client()
    
    # TODO: implement checking for blob_url later
    # def _validate_url(self):
    
    @log_function_call
    def _split_path(self, file_path):
        if file_path != '': splitted_path = os.path.split(file_path)
        else: splitted_path = file_path
        return splitted_path
    
    @log_function_call
    def process_blob_url(self):
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

    # TODO: implement this using managed identity, after the function have been deployed to Azure
    # def _check_for_credential(self):
    
    @log_function_call
    def create_blob_service_client(self):
        # TOOD: implement _check_for_credential into this function and handling error
        if not hasattr(self, "blob_info"):
            self.process_blob_url()
        if self.blob_info == "" or self.blob_info == None:
            logger.error("blob_info attribute is not identifed")
            raise AttributeError("blob_info attribute is not identifed")
        self.blob_service_client = BlobServiceClient(account_url=self.blob_info["account_url"], credential=self.credential)
        return self.blob_service_client

    @log_function_call
    def _ensure_blob_service_client(self):
        if not hasattr(self, "blob_service_client"):
            self.create_blob_client()
        return self.blob_service_client

    @log_function_call
    def create_container_client(self, container: str = None):
        # TODO: handling error
        self._ensure_blob_service_client()
        if container == None:
            self.blob_container_client = self.blob_service_client.get_container_client(self.blob_info["container_name"])
        else: self.blob_container_client = self.blob_service_client.get_container_client(container)
        return self.blob_container_client
    
    # @classmethod
    # @log_function_call
    # def upload_file(self, blob_name, filepath: str, container: str = None, metadata: dict[str, str] = None):
    #     # TODO: this function has to return state
    #     if container != None:
    #         self.create_container_client(container)
    #         if not self.blob_container_client.exists():
    #             self.blob_container_client.create_container()
    #         # TODO: handling error, double check on this code, this will get IO stream ouput from formrecoginzer
    #         with open(filepath, "rb") as data:
    #             if metadata == None:
    #                 self.blob_container_client.upload_blob(blob_name, data, overwrite=True)
    #             else:
    #                 self.blob_container_client.upload_blob(blob_name, data, metadata=metadata)
    #     else:
    #         logger.error("container is not defined")
    #         raise ValueError("container is not defined")

    @log_function_call
    def upload_file(self, blob_name, data: str, container: str = None, metadata: dict[str, str] = None):
        # TODO: this function has to return state
        if container == None:
            container = self.blob_info["container_name"]
        self.create_container_client(container)
        if not self.blob_container_client.exists():
            self.blob_container_client.create_container()
        # TODO: handling error, double check on this code, this will get IO stream ouput from formrecoginzer
        if metadata == None:
            self.blob_container_client.upload_blob(blob_name, data, overwrite=True)
        else:
            self.blob_container_client.upload_blob(blob_name, data, metadata=metadata)

    @log_function_call
    def remove_blobs(self, blob_path: str, container: str = ""):
        self.create_container_client(container)
        self.blob_container_client
        if self.blob_container_client.exists():
            self.blob_container_client.delete_blob(blob_path)
    
    @log_function_call
    def create_blob_client(self, file_path: str = None, container: str = None):
        self._ensure_blob_service_client(self)
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
