import os
import logging
import azure.functions as func
from dotenv import load_dotenv
from scripts.common import logger_config
from scripts.eventcreateandmodify import BlobCreateModifyEventHandler

app = func.FunctionApp()
logger_config()
logger = logging.getLogger("file")
load_dotenv()
openai_api_key = os.getenv("AZURE_OPENAI_KEY")
openai_api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
openai_deployment_id = os.getenv("AZURE_OPENAI_DEPLOYMENT")


@app.event_grid_trigger(arg_name="azeventgrid")
def BlobStorageTrigger(azeventgrid: func.EventGridEvent):
    event_json = azeventgrid.get_json()
    logger.debug(event_json)
    if event_json["api"].lower() == "putblob":
        event_handler = BlobCreateModifyEventHandler(blob_url=event_json["url"],
                                                     openai_api_key=openai_api_key,
                                                     openai_api_base=openai_api_base,
                                                     openai_deployment_id=openai_deployment_id)
        event_handler.split_and_blob_upload()
