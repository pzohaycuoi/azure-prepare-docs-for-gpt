import logging
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
        self.system_prompt_template = """you are an AI that help extract information from document.
            Your job is filling information into json provided below:
            
            '''{metadata}'''
            
            Those are required field, fill in with exact information from the document,
            don't replace the value of field or clear any value of any field if they are already exist,
            don't give any information other than the JSON,
            don't add any additional fields other than provided field like product, responsibilities,...
            if field already have value then retain them, only fill empty fields,
            buyerName and sellerName must be a company name, A and B dooes not present the company name,
            your reply is only this json with the data filled in,
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
        system_prompt = self.system_prompt_template
        system_prompt = system_prompt.format(metadata=self.metadata)
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
    def _openai_chat(self, messages: list[dict[str, str]]):
        try:
            self.chat = openai.ChatCompletion.create(engine=self.deployment_id,
                                                     messages=messages,
                                                     temperature=0.2,
                                                     frequency_penalty=0,
                                                     presence_penalty=0,
                                                     max_tokens=800,
                                                     top_p=0.95)
        except Exception as e:
            logger.error(e)
            raise e
        return self.chat

    @log_function_call
    def _format_json(self, text):
        fixed_json = fix_json(text)
        result = {}
        fixed_json_key = fixed_json.keys()
        for ele in self.metadata_list:
            if ele not in fixed_json_key:
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


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    load_dotenv()
    api_key = os.getenv("AZURE_OPENAI_KEY")
    api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
    deployment_id = os.getenv("AZURE_OPENAI_DEPLOYMENT")
    splitter = DocumentSplitter(api_key=api_key, api_base=api_base, deployment_id=deployment_id)
    document = "/Users/nam/Downloads/generic_contract_2.pdf"
    res = splitter.split(document)
    print(res)
