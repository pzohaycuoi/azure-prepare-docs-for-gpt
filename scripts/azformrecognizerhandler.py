import os
import logging
from common import logger_config, log_function_call
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient


logger_config()
logger = logging.getLogger("file")

class FormRecognizerHandler():
    def __init__(self):
        endpoint = os.getenv("AZ_FORMRECOGNIZER_ENDPOINT")
        if endpoint == None or endpoint == "":
            logger.error("Form Recognizer endpoint is not defined")
            raise ValueError("Form Recognizer endpoint is not defined")
        else:
            self.service_info = {"endpoint": endpoint, "api_verison": "2023-02-28-preview"}
    
    @log_function_call
    def _ensure_crendetial(self, sp: ClientSecretCredential = None):
        """
        Ensures the existence of the credential attribute.

        This method checks if the credential attribute exists. If it doesn't, it follows the
        following steps to set the credential attribute:
        1. Checks if the environment variable AZ_FORMRECOGNIZER_KEY is set. If it is, uses
        AzureKeyCredential with the value of the environment variable as the credential.
        2. If the environment variable is not set, checks if the sp (Service Principal) parameter
        is provided. If it is, uses it as the credential.
        3. If neither the environment variable nor the sp parameter is set, uses DefaultAzureCredential
        to automatically obtain the appropriate credential.

        Returns:
            TokenCredential: The credential attribute.
        """
        if not hasattr(self, "credential"):
            env_cred = os.getenv("AZ_FORMRECOGNIZER_KEY")
            if env_cred == None:
                if sp == None: self.credential = DefaultAzureCredential()
                else: self.credential = sp
            else: self.credential = AzureKeyCredential(env_cred)

        return self.credential
    
    @log_function_call
    def create_formrecognizer_client(self):
        """
        Creates and returns a FormRecognizer client.

        This method ensures the existence of the credential attribute by calling the
        _ensure_crendetial method. Then, it creates a DocumentAnalysisClient using the
        endpoint from the service_info dictionary and the credential. Finally, it returns
        the created FormRecognizer client.

        Returns:
            DocumentAnalysisClient: The created FormRecognizer client.
        """
        self._ensure_crendetial()
        self.formrecognizer_client = DocumentAnalysisClient(endpoint=self.service_info["endpoint"], credential=self.credential)
        return self.formrecognizer_client
    
    @log_function_call
    def _ensure_formrecognizer_client(self):
        """
        Ensures the existence of the formrecognizer_client attribute.

        This method checks if the formrecognizer_client attribute exists. If it doesn't, it calls
        the create_formrecognizer_client method to generate and set the formrecognizer_client attribute.
        If the formrecognizer_client attribute is still not existent after the call, an error is logged,
        and an AttributeError is raised.

        Returns:
            FormRecognizerClient: The formrecognizer_client attribute.

        Raises:
            AttributeError: If the formrecognizer_client attribute is not existent.
        """
        if not hasattr(self, "formrecognizer_client"):
            self.create_formrecognizer_client()

        if not hasattr(self, "formrecognizer_client") or self.formrecognizer_client is None:
            logger.error("Form recognizer client does not exist")
            raise AttributeError("Form recognizer client does not exist")

        return self.formrecognizer_client
    
    def analyze_document(self, document_url):
        """
        Analyzes a document located at the specified URL.

        This method ensures the existence of the formrecognizer_client attribute by calling the
        _ensure_formrecognizer_client method. Then, it initiates the document analysis by calling
        begin_analyze_document_from_url on the formrecognizer_client with the model_id and document_url
        parameters. It waits for the analysis result by calling poller.result(). Finally, it returns
        the analysis result.

        Args:
            document_url (str): The URL of the document to analyze.

        Returns:
            DocumentAnalysisResult: The result of the document analysis.

        Raises:
            Exception: If an error occurs during document analysis.

        """
        try:
            self._ensure_formrecognizer_client()
            poller = self.formrecognizer_client.begin_analyze_document_from_url(model_id="prebuilt-document", document_url=document_url)
            self.result = poller.result()
            return self.result
        except Exception as e:
            logger.error("Error occurred during document analysis: %s", str(e))
            raise
