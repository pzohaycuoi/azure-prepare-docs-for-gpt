import logging
from retry import retry
from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
import openai
from common import logger_config, log_function_call
from utils import fix_json


logger_config()
logger = logging.getLogger("file")


class DocumentSplitter():

    def __init__(self, api_key: str, api_base: str, deployment_id: str) -> None:
        openai.api_type = "azure"
        openai.api_key = api_key
        openai.api_base = api_base
        openai.api_version = "2023-03-15-preview"  # subject to change
        self.deployment_id = deployment_id
        self.metadata_list = [
            "contractName",
            "contractID",
            "contractDate",
            "buyerName",
            "sellerName"
        ]
        
        self.few_shot_metadata = '''{
            "contractName": "<contractname>",
            "contractID": "<contractid-withoutspace>",
            "contractDate": "<ddmmyyyy>",
            "buyerName": "<companyname>",
            "sellerName": "<companyname>"
        }'''
        
        self.metadata = '''{
            "contractName": "<contractname>",
            "contractID": "<contractid-withoutspace>",
            "contractDate": "<ddmmyyyy>",
            "buyerName": "<companyname>",
            "sellerName": "<companyname>"
        }'''
        self.messages = []
        self.system_message = {
            "role": "system",
            "content": ""
        }
        self.system_prompt_template = """you are an AI that help extract information from document and translate them to english.
            user provide a json contains all of the informations they need, they don't need any other informations, do not fill in other information that does not match
            and your job is fill in information into the json user provided below:
            
            '''{metadata}'''

            before filling in, translate informations from the document into english,
            don't add any new information into the provided json, if nothing new to fill in return the same json as provided,
            don't replace the value of field or clear any value of any field if they are already exist, but if it not in english you must translate it to english
            don't give any information other than the JSON, your reply is only this json with the data filled in.
            Below is the format for the answer, use this to reply to the user:
            
            '''{few_shot_metadata}'''
            
            If number of field not matching with the above format then get rid of fields not matching.
            The user will provide paragraph of the document, that need to extract information."""

    @log_function_call
    def _document_splitter(self, document):
        loader = PyPDFLoader(document)
        pages = loader.load_and_split()
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1400, chunk_overlap=200)
        chunks = text_splitter.split_documents(pages)
        return chunks

    @log_function_call
    def _construct_system_message(self, metadata: str = None):
        if metadata != None:
            self.metadata = metadata
        system_prompt = self.system_prompt_template.format(metadata=self.metadata,
                                                           few_shot_metadata=self.few_shot_metadata)
        self.system_message["content"] = system_prompt
        if len(self.messages) == 0:
            self.messages.append(self.system_message)
        else:
            self.messages[0] = self.system_message
        return system_prompt

    @log_function_call
    def _next_message(self, message=None, history=None):
        def _condition_append(user=None, assisant=None):
            if user == None and not assisant == None:
                self.messages.append({"role": "assistant", "content": f"{assisant}"})
            if not user == None and assisant == None:
                self.messages.append({"role": "user", "content": f"{user}"})

        if len(self.messages) < 4:
            _condition_append(message, history)
        else:
            del self.messages[1]
            _condition_append(message, history)
        return self.messages

    @log_function_call
    @retry(Exception, tries=4, delay=2, backoff=2)
    def _openai_chat(self, messages: list[dict[str, str]], max_tries: int = 3):
        self.chat = openai.ChatCompletion.create(engine=self.deployment_id,
                                                 messages=messages,
                                                 temperature=0.2,
                                                 frequency_penalty=0,
                                                 presence_penalty=0,
                                                 max_tokens=800,
                                                 top_p=0.95)
        return self.chat

    @log_function_call
    def _format_json(self, text):
        fixed_json = fix_json(text)
        result = {}
        for ele in self.metadata_list:
            if ele not in fixed_json.keys():
                logger.error(f"{ele} not in json")
                raise ValueError(f"{ele} not in json")
            else:
                result.update({f"{ele}": fixed_json[ele]})
        return result

    @log_function_call
    def split(self, document):
        chunks = self._document_splitter(document)
        result = {"metadata": "", "chunks": []}
        self._construct_system_message()
        for chunk in chunks:
            result["chunks"].append(chunk.page_content)
            logger.debug(chunk.metadata)
            self._next_message(message=chunk.page_content)
            self.chat = self._openai_chat(messages=self.messages)
            self._construct_system_message(self.chat.choices[0].message.content)
            self._next_message(history=self.chat.choices[0].message.content)
        result["metadata"] = self._format_json(self.chat.choices[0].message.content)
        return result
