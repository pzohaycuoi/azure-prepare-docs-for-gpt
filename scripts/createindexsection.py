import os
import re
import logging
from common import logger_config, log_function_call
from azblobhanlder import BlobHandler
from documentprocessing import ProcessDocument
from azformrecognizerhandler import FormRecognizerHandler
from azcognitivesearchhandler import CognitiveSearchHandler


logger_config()
logger = logging.getLogger("file")

@log_function_call
def create_document_index(blob_url: str):
    
    @log_function_call
    def _blob_name_from_file_page(filename, page=0):
        if os.path.splitext(filename)[1].lower() == ".pdf":
            return os.path.splitext(os.path.basename(filename))[0] + f"-{page}" + ".pdf"
        else:
            return os.path.basename(filename)
    
    @log_function_call
    def _create_sections(filename: str, sections):
        for i, (section, pagenum) in enumerate(sections):
            yield {
                "id": re.sub("[^0-9a-zA-Z_-]", "_", f"{filename}-{i}"),
                "content": section,
                # "category": args.category,
                "sourcepage": _blob_name_from_file_page(filename, pagenum),
                "sourcefile": filename
            }

    blob_proc = BlobHandler(blob_url=blob_url)
    form_recog_proc = FormRecognizerHandler()
    form_recog_proc.analyze_document(blob_proc.blob_url)
    document_proc = ProcessDocument(form_recognizer_results=form_recog_proc.result)
    document_proc.split_text()
    sections = _create_sections(blob_proc.blob_info["blob_name"])
    cog_search_proc = CognitiveSearchHandler()
    cog_search_proc.upload_index_document(filename=blob_proc.blob_info["blob_name"], sections=sections)
