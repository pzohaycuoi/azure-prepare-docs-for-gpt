import os
import time
import logging
from common import logger_config, log_function_call
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchIndex, SimpleField, SearchableField, SemanticSettings, SemanticConfiguration, PrioritizedFields, SemanticField
from azure.search.documents import SearchClient


logger_config()
logger = logging.getLogger("file")

class CognitiveSearchHandler():
    
    def __init__(self):
        # TODO: THIS IS NOT THE WAY TO DO IT
        self.credential = AzureKeyCredential(os.getenv("AZURE_COGNITIVESEARCH_KEY"))
        self.endpoint = os.getenv("AZURE_COGNITIVESEARCH_ENDPOINT")
        self.index_name = os.getenv("AZURE_COGNITIVESEARCH_INDEXNAME")
            
    # TODO: implement this later with managede identity for azure function, no time right now
    # def _ensure_crential(self):
    
    @log_function_call
    def _create_search_index_client(self):
        self.index_client = SearchIndexClient(endpoint=self.endpoint, credential=self.credential)
        return self.index_client
    
    @log_function_call
    def create_search_index(self):
        logger.debug(f"Ensuring search index {self.index_name} exists")
        self._create_search_index_client()
        if self.index_name not in self.index_client.list_index_names():
            search_index = SearchIndex(
                name=self.index_name,
                fields=[
                    SimpleField(name="id", type="Edm.String", key=True),
                    SearchableField(name="content", type="Edm.String", analyzer_name="en.microsoft"),
                    SimpleField(name="category", type="Edm.String", filterable=True, facetable=True),
                    SimpleField(name="sourcepage", type="Edm.String", filterable=True, facetable=True),
                    SimpleField(name="sourcefile", type="Edm.String", filterable=True, facetable=True)
                ],
                semantic_settings=SemanticSettings(
                    configurations=[SemanticConfiguration(
                        name='default',
                        prioritized_fields=PrioritizedFields(
                            title_field=None, prioritized_content_fields=[SemanticField(field_name='content')]))])
            )
            logger.debug(f"Creating {self.index_name} search index")
            self._index_client.create_index(search_index)
        else:
            logger.debug(f"Search index {self.index_name} already exists")
    
    @log_function_call
    def _create_search_client(self):
        self.create_search_index()
        self.search_client = SearchClient(endpoint=self.endpoint, index_name=self.index_name, credential=self.credential)
        return self.search_client
            
    @log_function_call
    def upload_index_document(self, filename: str, sections: any):
        logger.debug(f"Indexing sections from '{filename}' into search index '{self.index_name}'")
        self._create_search_client(index_name=self.index_name)
        i = 0
        batch = []
        for s in sections:
            batch.append(s)
            i += 1
            if i % 1000 == 0:
                results = self._search_client.upload_documents(documents=batch)
                succeeded = sum([1 for r in results if r.succeeded])
                logger.debug(f"\tIndexed {len(results)} sections, {succeeded} succeeded")
                batch = []

        if len(batch) > 0:
            results = self._search_client.upload_documents(documents=batch)
            succeeded = sum([1 for r in results if r.succeeded])
            logger.debug(f"\tIndexed {len(results)} sections, {succeeded} succeeded")

    @log_function_call
    def remove_from_index(self, filename: str = None):
        logger.debug(f"Removing sections from '{filename or '<all>'}' from search index '{self.index_name}'")
        self._create_search_client(index_name=self.index_name)
        while True:
            filter = None if filename == None else f"sourcefile eq '{os.path.basename(filename)}'"
            r = self._search_client.search("", filter=filter, top=1000, include_total_count=True)
            if r.get_count() == 0:
                break
            r = self._search_client.delete_documents(documents=[{"id": d["id"]} for d in r])
            logger.debug(f"\tRemoved {len(r)} sections from index")
            # It can take a few seconds for search results to reflect changes, so wait a bit
            time.sleep(2)
