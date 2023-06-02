import logging
import azure.functions as func
from dotenv import load_dotenv
from scripts.common import logger_config, log_function_call
from scripts.createindexsection import create_document_index


app = func.FunctionApp()
logger_config()
logger = logging.getLogger("file")
load_dotenv()


@app.event_grid_trigger(arg_name="azeventgrid")
def BlobStorageTrigger(azeventgrid: func.EventGridEvent):
    event_json = azeventgrid.get_json()
    logger.debug(event_json)
    if event_json["api"].lower() == "putblob":
        create_document_index(event_json["url"])
