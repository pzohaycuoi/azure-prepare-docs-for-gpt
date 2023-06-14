import os
import logging
from dotenv import load_dotenv
from azure.identity import ChainedTokenCredential, ManagedIdentityCredential
from azure.core.credentials import AzureSasCredential
from documentsplitter import DocumentSplitter
from azblobhanlder import BlobHandler
from common import logger_config, log_function_call


logger_config()
logger = logging.getLogger("file")
load_dotenv()


class BlobCreateModifyEventHandler:
    def __init__(self, blob_url: str,  # credential: ManagedIdentityCredential
                 openai_api_key: str, openai_api_base: str, openai_deployment_id: str):
        self.blob_url = blob_url
        # self.managed_identity_credential = ManagedIdentityCredential()
        # self.chained_credential = ChainedTokenCredential(self.managed_identity_credential)
        self.doc_splitter = DocumentSplitter(api_key=openai_api_key,
                                             api_base=openai_api_base,
                                             deployment_id=openai_deployment_id)
        # self.blob_handler = BlobHandler(blob_url=self.blob_url,
        #                                 credential=self.chained_credential)
        self.blob_handler = BlobHandler(blob_url=self.blob_url, credential=os.getenv("AZURE_STORAGEACCOUNT_SAS"))

    @log_function_call
    def _blob_name_from_file_page(self, filename, page=0):
        if os.path.splitext(filename)[1].lower() == ".pdf":
            return os.path.splitext(os.path.basename(filename))[0] + f"-{page}" + ".pdf"
        else:
            return os.path.basename(filename)

    @log_function_call
    def split_and_blob_upload(self):
        splitted_doc = self.doc_splitter.split(self.blob_url)
        metdata = splitted_doc["metadata"]
        chunks = splitted_doc["chunks"]
        filename = self.blob_handler.blob_info["blob_name"]
        i = 0
        for chunk in chunks:
            blob_name = self._blob_name_from_file_page(filename, i)
            i += 1
            self.blob_handler.upload_file(blob_name=blob_name, data=chunk, container="index", metadata=metdata)
