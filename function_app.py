import logging
import azure.functions as func
from dotenv import load_dotenv
from scripts.prepdocs import 


app = func.FunctionApp()
load_dotenv()

@app.event_grid_trigger(arg_name="azeventgrid")
def BlobStorageTrigger(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')
    logging.info(azeventgrid.get_json())

