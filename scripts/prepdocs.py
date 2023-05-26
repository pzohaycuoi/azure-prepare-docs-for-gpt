import argparse
import os
import glob
import html
import io
import re
import time
import datetime
import shutil
from pypdf import PdfReader, PdfWriter
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import *
from azure.search.documents import SearchClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from dotenv import load_dotenv
import pandas as pd
import pdfkit


MAX_SECTION_LENGTH = 1000
SENTENCE_SEARCH_LIMIT = 100
SECTION_OVERLAP = 100

parser = argparse.ArgumentParser(
    description="Prepare documents by extracting content from PDFs, splitting content into sections, uploading to blob storage, and indexing in a search index.",
    epilog="Example: prepdocs.py '..\data\*' --storageaccount myaccount --container mycontainer --searchservice mysearch --index myindex -v"
)
parser.add_argument("--files", required=False, help="Files to be processed")
parser.add_argument("--category", help="Value for the category field in the search index for all sections indexed in this run")
parser.add_argument("--skipblobs", action="store_true", help="Skip uploading individual pages to Azure Blob Storage")
parser.add_argument("--storageaccount", required=False, help="Azure Blob Storage account name")
parser.add_argument("--container", required=False, help="Azure Blob Storage container name")
parser.add_argument("--storagekey", required=False, help="Optional. Use this Azure Blob Storage account key instead of the current user identity to login (use az login to set current user for Azure)")
parser.add_argument("--tenantid", required=False, help="Optional. Use this to define the Azure directory where to authenticate)")
parser.add_argument("--searchservice", help="Name of the Azure Cognitive Search service where content should be indexed (must exist already)")
parser.add_argument("--index", required=False, help="Name of the Azure Cognitive Search index where content should be indexed (will be created if it doesn't exist)")
parser.add_argument("--searchkey", required=False, help="Optional. Use this Azure Cognitive Search account key instead of the current user identity to login (use az login to set current user for Azure)")
parser.add_argument("--remove", action="store_true", help="Remove references to this document from blob storage and the search index")
parser.add_argument("--removeall", action="store_true", help="Remove all blobs from blob storage and documents from the search index")
parser.add_argument("--localpdfparser", action="store_true", help="Use PyPdf local PDF parser (supports only digital PDFs) instead of Azure Form Recognizer service to extract text, tables and layout from the documents")
parser.add_argument("--formrecognizerservice", required=False, help="Optional. Name of the Azure Form Recognizer service which will be used to extract text, tables and layout from the documents (must exist already)")
parser.add_argument("--formrecognizerkey", required=False, help="Optional. Use this Azure Form Recognizer account key instead of the current user identity to login (use az login to set current user for Azure)")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
args = parser.parse_args()


def blob_name_from_file_page(filename, page=0):
    """
    Create a blob name from a file name and page number

    Parameters
    ----------
    filename : str
        File name
    page : int
        Page number

    Returns
    -------
    str
        Blob name
    """
    if os.path.splitext(filename)[1].lower() == ".pdf":
        return os.path.splitext(os.path.basename(filename))[0] + f"-{page}" + ".pdf"
    else:
        return os.path.basename(filename)


def upload_blobs(filename):
    """
    Upload a file to Azure Blob Storage

    Parameters
    ----------
    filename : str
        File name

    Returns
    -------
    None
    """
    blob_service = BlobServiceClient(account_url=f"https://{storageaccount}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(container)
    if not blob_container.exists():
        blob_container.create_container()

    # if file is PDF split into pages and upload each page as a separate blob
    if os.path.splitext(filename)[1].lower() == ".pdf":
        reader = PdfReader(filename)
        pages = reader.pages
        for i in range(len(pages)):
            blob_name = blob_name_from_file_page(filename, i)
            if args.verbose: print(f"\tUploading blob for page {i} -> {blob_name}")
            f = io.BytesIO()
            writer = PdfWriter()
            writer.add_page(pages[i])
            writer.write(f)
            f.seek(0)
            blob_container.upload_blob(blob_name, f, overwrite=True)
    else:
        blob_name = blob_name_from_file_page(filename)
        with open(filename, "rb") as data:
            blob_container.upload_blob(blob_name, data, overwrite=True)


def remove_blobs(filename):
    """
    Remove blobs from Azure Blob Storage

    Parameters
    ----------
    filename : str
        File name

    Returns
    -------
    None
    """
    if args.verbose: print(f"Removing blobs for '{filename or '<all>'}'")
    blob_service = BlobServiceClient(account_url=f"https://{storageaccount}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(container)
    if blob_container.exists():
        if filename == None:
            blobs = blob_container.list_blob_names()
        else:
            prefix = os.path.splitext(os.path.basename(filename))[0]
            blobs = filter(lambda b: re.match(f"{prefix}-\d+\.pdf", b), blob_container.list_blob_names(name_starts_with=os.path.splitext(os.path.basename(prefix))[0]))
        for b in blobs:
            if args.verbose: print(f"\tRemoving blob {b}")
            blob_container.delete_blob(b)


def table_to_html(table):
    """
    Convert a table to HTML

    Parameters
    ----------
    table : Table
        Table

    Returns
    -------
    str
        HTML
    """
    
    table_html = "<table>"
    rows = [sorted([cell for cell in table.cells if cell.row_index == i], key=lambda cell: cell.column_index) for i in range(table.row_count)]
    for row_cells in rows:
        table_html += "<tr>"
        for cell in row_cells:
            tag = "th" if (cell.kind == "columnHeader" or cell.kind == "rowHeader") else "td"
            cell_spans = ""
            if cell.column_span > 1: cell_spans += f" colSpan={cell.column_span}"
            if cell.row_span > 1: cell_spans += f" rowSpan={cell.row_span}"
            table_html += f"<{tag}{cell_spans}>{html.escape(cell.content)}</{tag}>"
        table_html += "</tr>"
    table_html += "</table>"
    return table_html


def get_document_text(filename):
    """
    Get the text from a document

    Parameters
    ----------
    filename : str
        File name

    Returns
    -------
    str
        Document text

    list
        List of tuples containing page number, offset, and text for each page
    """
    offset = 0
    page_map = []
    if args.localpdfparser:
        reader = PdfReader(filename)
        pages = reader.pages
        for page_num, p in enumerate(pages):
            page_text = p.extract_text()
            page_map.append((page_num, offset, page_text))
            offset += len(page_text)
    else:
        if args.verbose: print(f"Extracting text from '{filename}' using Azure Form Recognizer")
        form_recognizer_client = DocumentAnalysisClient(endpoint=f"https://{formrecognizerservice}.cognitiveservices.azure.com/", credential=formrecognizer_creds, headers={"x-ms-useragent": "azure-search-chat-demo/1.0.0"})
        with open(filename, "rb") as f:
            poller = form_recognizer_client.begin_analyze_document("prebuilt-layout", document=f)
        form_recognizer_results = poller.result()

        #  for each page in the document, extract the text and the tables
        for page_num, page in enumerate(form_recognizer_results.pages):
            # extract text from the page
            page_text = ""
            for line in page.lines:
                for word in line.words:
                    page_text += word.text + " "
            page_text += " "
            page_map.append((page_num, offset, page_text))
            offset += len(page_text)

            # extract tables from the page
            # first, find the table spans in the page
            table_spans = []
            for table in form_recognizer_results.tables:
                for span in table.spans:
                    if span.page_number == page_num + 1:
                        table_spans.append(span)

            # sort table spans by offset
            table_spans = sorted(table_spans, key=lambda span: span.offset)

            # for each table span, extract the table and add it to the page text
            for span in table_spans:
                page_text += table_to_html(form_recognizer_results.tables[span.table_id])
            page_text += " "
            tables_on_page = [table for table in form_recognizer_results.tables if table.bounding_regions[0].page_number == page_num + 1]

            # mark all positions of the table spans in the page
            page_offset = page.spans[0].offset
            page_length = page.spans[0].length
            table_chars = [-1]*page_length
            for table_id, table in enumerate(tables_on_page):
                for span in table.spans:
                    # replace all table spans with "table_id" in table_chars array
                    for i in range(span.length):
                        idx = span.offset - page_offset + i
                        if idx >= 0 and idx < page_length:
                            table_chars[idx] = table_id

            # build page text by replacing charcters in table spans with table html
            page_text = ""
            added_tables = set()
            for idx, table_id in enumerate(table_chars):
                if table_id == -1:
                    page_text += form_recognizer_results.content[page_offset + idx]
                elif table_id not in added_tables:
                    page_text += table_to_html(tables_on_page[table_id])
                    added_tables.add(table_id)

            page_text += " "
            page_map.append((page_num, offset, page_text))
            offset += len(page_text)

    return page_map


def split_text(page_map):
    """
    Split text into sections

    Parameters
    ----------
    page_map : list
        List of tuples (page number, offset, text)

    Returns
    -------
    list
        List of tuples (page number, offset, text)

    Notes
    -----
    This function splits the text into sections of length MAX_SECTION_LENGTH, with a minimum overlap of SECTION_OVERLAP characters.
    It tries to split on sentence endings, and if that is not possible, on word breaks.

    The function also tries to split on page boundaries, but this is not always possible.
    """
    SENTENCE_ENDINGS = [".", "!", "?"]
    WORDS_BREAKS = [",", ";", ":", " ", "(", ")", "[", "]", "{", "}", "\t", "\n"]
    if args.verbose: print(f"Splitting '{filename}' into sections")

    def find_page(offset):
        leng = len(page_map)
        for i in range(leng - 1):
            if offset >= page_map[i][1] and offset < page_map[i + 1][1]:
                return i
        return leng - 1

    all_text = "".join(p[2] for p in page_map)
    length = len(all_text)
    start = 0
    end = length
    while start + SECTION_OVERLAP < length:
        last_word = -1
        end = start + MAX_SECTION_LENGTH

        if end > length:
            end = length
        else:
            # Try to find the end of the sentence or at least a whole word boundary
            while end < length and (end - start - MAX_SECTION_LENGTH) < SENTENCE_SEARCH_LIMIT and all_text[end] not in SENTENCE_ENDINGS:
                if all_text[end] in WORDS_BREAKS:
                    last_word = end
                end += 1
            if end < length and all_text[end] not in SENTENCE_ENDINGS and last_word > 0:
                end = last_word  # Fall back to at least keeping a whole word
        if end < length:
            end += 1

        # Try to find the start of the sentence or at least a whole word boundary
        last_word = -1
        while start > 0 and start > end - MAX_SECTION_LENGTH - 2 * SENTENCE_SEARCH_LIMIT and all_text[start] not in SENTENCE_ENDINGS:
            if all_text[start] in WORDS_BREAKS:
                last_word = start
            start -= 1
        if all_text[start] not in SENTENCE_ENDINGS and last_word > 0:
            start = last_word
        if start > 0:
            start += 1

        section_text = all_text[start:end]
        yield (section_text, find_page(start))

        last_table_start = section_text.rfind("<table")
        if (last_table_start > 2 * SENTENCE_SEARCH_LIMIT and last_table_start > section_text.rfind("</table")):
            # If the section ends with an unclosed table, we need to start the next section with the table.
            # If table starts inside SENTENCE_SEARCH_LIMIT, we ignore it, as that will cause an infinite loop for tables longer than MAX_SECTION_LENGTH
            # If last table starts inside SECTION_OVERLAP, keep overlapping
            if args.verbose:
                print(f"Section ends with unclosed table, starting next section with the table at page {find_page(start)} offset {start} table start {last_table_start}")
            start = min(end - SECTION_OVERLAP, start + last_table_start)
        else:
            start = end - SECTION_OVERLAP

    if start + SECTION_OVERLAP < end:
        yield (all_text[start:end], find_page(start))


def create_sections(filename, page_map):
    """
    Create sections from text

    Parameters
    ----------
    filename : str
        Name of the file
    page_map : list
        List of tuples (page number, offset, text)

    Returns
    -------
    list
        List of dictionaries with section information

    Notes
    -----
    This function creates a section for each section of text returned by split_text.
    It also adds the page number and offset of the first page of the section to the section information.
    The section id is a combination of the filename and the section number.
    The section content is the text of the section.
    The category is the category specified on the command line.
    The sourcepage is the name of the page blob containing the first page of the section.
    The sourcefile is the name of the file.
    """
    for i, (section, pagenum) in enumerate(split_text(page_map)):
        yield {
            "id": re.sub("[^0-9a-zA-Z_-]", "_", f"{filename}-{i}"),
            "content": section,
            "category": args.category,
            "sourcepage": blob_name_from_file_page(filename, pagenum),
            "sourcefile": filename
        }


def create_search_index(index_name):
    """
    Create search index
    """
    if args.verbose: print(f"Ensuring search index {index_name} exists")
    index_client = SearchIndexClient(endpoint=f"https://{searchservice}.search.windows.net/",
                                     credential=search_creds)
    if index_name not in index_client.list_index_names():
        search_index = SearchIndex(
            name=index_name,
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
        if args.verbose: print(f"Creating {index_name} search index")
        index_client.create_index(search_index)
    else:
        if args.verbose: print(f"Search index {index_name} already exists")


def index_sections(filename, sections):
    """
    Index sections

    Parameters
    ----------
    filename : str
        The name of the file containing the sections
    sections : list
        A list of sections to index

    Notes
    -----
    This function indexes the sections into the search index specified on the command line.
    The sections are indexed in batches of 1000.
    The sections are indexed using the upload_documents method of the SearchClient class.
    The upload_documents method returns a list of results.
    The succeeded property of the results indicates whether the indexing succeeded.
    The key property of the results contains the id of the section.
    The status_code property of the results contains the HTTP status code of the indexing operation.
    The error property of the results contains an error message if the indexing failed.
    """
    if args.verbose: print(f"Indexing sections from '{filename}' into search index '{index}'")
    search_client = SearchClient(endpoint=f"https://{searchservice}.search.windows.net/", index_name=index, credential=search_creds)
    i = 0
    batch = []
    for s in sections:
        batch.append(s)
        i += 1
        if i % 1000 == 0:
            results = search_client.upload_documents(documents=batch)
            succeeded = sum([1 for r in results if r.succeeded])
            if args.verbose: print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")
            batch = []

    if len(batch) > 0:
        results = search_client.upload_documents(documents=batch)
        succeeded = sum([1 for r in results if r.succeeded])
        if args.verbose: print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")


def remove_from_index(filename):
    """
    Remove sections from the search index

    Parameters
    ----------
    filename : str
        The name of the file containing the sections to remove

    Notes
    -----
    This function removes the sections from the search index specified on the command line.
    The sections are removed in batches of 1000.
    The sections are removed using the delete_documents method of the SearchClient class.
    The delete_documents method returns a list of results.
    The succeeded property of the results indicates whether the removal succeeded.
    The key property of the results contains the id of the section.
    The status_code property of the results contains the HTTP status code of the removal operation.
    The error property of the results contains an error message if the removal failed.
    """
    if args.verbose: print(f"Removing sections from '{filename or '<all>'}' from search index '{index}'")
    search_client = SearchClient(endpoint=f"https://{searchservice}.search.windows.net/", index_name=index, credential=search_creds)
    while True:
        filter = None if filename == None else f"sourcefile eq '{os.path.basename(filename)}'"
        r = search_client.search("", filter=filter, top=1000, include_total_count=True)
        if r.get_count() == 0:
            break
        r = search_client.delete_documents(documents=[{"id": d["id"]} for d in r])
        if args.verbose: print(f"\tRemoved {len(r)} sections from index")
        # It can take a few seconds for search results to reflect changes, so wait a bit
        time.sleep(2)


def convert_excel_to_pdf(file):
    """
    Convert excel file to pdf

    Parameters
    ----------
    file : str
        The excel file to convert

    Returns
    -------
    str
        The converted pdf file

    Notes
    -----
    This function is used to convert the excel file to pdf file
    """
    # Get the file name
    file_parts = os.path.split(file)
    dir_path = file_parts[0]
    file_name = file_parts[1]

    # Convert to html
    df = pd.read_excel(file)
    df.fillna('', inplace=True)
    for col in df:
        if "Unnamed" in col:
            df.rename(columns={col: ''}, inplace=True)

    temp_path = "./temp.html"
    html = df.to_html(index=False)
    with open(temp_path, 'w', encoding="utf-8") as f:
        f.writelines('<meta charset="UTF-8">\n')
        f.write(html)

    # Convert to pdf
    out_file = os.path.join(dir_path, file_name.replace(".xlsx", ".pdf"))
    pdfkit.from_file(temp_path, out_file)
    # remove the temp file
    os.remove(temp_path)
    return out_file


def generate_file_name(file_path):
    """
    Genereate file name with timestamp appended,
    If parameter is path, return a path with new file name
    Else if parameter is a file name, return only new file name

    Parameters
    ----------
    file_path : str
        The file path or file name to convert

    Returns
    -------
    str
        The converted file name or file path
    """
    if len(os.path.split(file_path)) > 1:
        path_parts = os.path.split(file_path)
        dir_path = path_parts[0]
        file_name = path_parts[1]
        file_name_parts = os.path.splitext(file_name)
        time_stamp = datetime.datetime.now().strftime('%d%m%y-%H%M%S')
        new_file_name = f"{file_name_parts[0]}-{time_stamp}{file_name_parts[1]}"
        new_path = os.path.join(dir_path, new_file_name)
        return new_path
    else:
        file_name_parts = os.path.splitext(file_name)
        time_stamp = datetime.datetime.now().strftime('%d%m%y-%H%M%S')
        new_file_name = f"{file_name_parts[0]}-{time_stamp}{file_name_parts[1]}"
        return new_file_name


def convert_to_realpath(path):
    """
    Get the current workspace should be inside the report-automation folder
    Convert the path in arg to absolute path with relative path appended
    So path using relative path should be working normally

    Parameters
    ----------
    path : str
        The path to convert

    Returns
    -------
    str
        The converted path

    Notes
    -----
    This function is used to convert the relative path to absolute path
    """
    workspace_dir = os.path.dirname(
        os.path.dirname(os.path.realpath(__file__)))
    if workspace_dir not in path:
        current_path = (os.path.dirname(os.path.realpath(__file__)))
        real_path = os.path.join(current_path, path)
        return real_path
    else:
        return path


def move_file_to_path(file, path):
    """
    Move file to path, check if the path is exist or not
    if not exist, move the file to the path
    if yes, move the file to the path with new name

    Parameters
    ----------
    file : str
        The file to move
    path : str
        The path to move the file to

    Returns
    -------
    str
        The new path of the file
    """
    workspace_dir = os.path.dirname(
        os.path.dirname(os.path.realpath(__file__)))

    if workspace_dir not in path:
        real_path = convert_to_realpath(path)
    else:
        real_path = path

    # combine path with file name to check existence of the file in new path and nove file
    file_new_path = os.path.join(real_path, os.path.basename(file))
    if os.path.exists(file_new_path):
        file_new_path = generate_file_name(file_new_path)
        shutil.move(file, file_new_path)
    else:
        shutil.move(file, file_new_path)

    return file_new_path


def move_file_to_temp(file):
    """
    Move file to temp folder, check if the temp folder is exist or not
    if not exist, create the temp folder and move the file to the temp folder
    if yes, move the file to the temp folder with new name
    """
    temp_path = convert_to_realpath("../temp/")
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    move_file_to_path(file, temp_path)


def proccess_excel_file(file):
    """
    File contains excel extension then use convert_excel_to_pdf function
    Excel file extension are ("xlsx", "xls")

    Parameters
    ----------
    file : str
        The file to process

    Returns
    -------
    str
        The processed file
    """
    if file.endswith(".xlsx") or file.endswith(".xls"):
        proced_file = convert_excel_to_pdf(file)
        # temp_path = convert_to_realpath("../temp/")
        # if not os.path.exists(temp_path):
        #     os.makedirs(temp_path)

        # move_file_to_path(file, temp_path)
        move_file_to_temp(file)
    else:
        proced_file = file
    return proced_file


load_dotenv()
# use environment variables if not provided as arguments
storageaccount = os.getenv("AZURE_STORAGE_ACCOUNT") if args.storageaccount == None else args.storageaccount
container = os.getenv("AZURE_STORAGE_CONTAINER") if args.container == None else args.container
searchservice = os.getenv("AZURE_SEARCH_SERVICE") if args.searchservice == None else args.searchservice
index = os.getenv("AZURE_SEARCH_INDEX") if args.index == None else args.index
formrecognizerservice = os.getenv("AZURE_FORMRECOGNIZER_SERVICE") if args.formrecognizerservice == None else args.formrecognizerservice
tenant_id = os.getenv("AZURE_TENANT_ID") if args.tenantid == None else args.tenantid

# use default credential if not provided as arguments
az_credential = DefaultAzureCredential()
search_key = AzureKeyCredential(os.getenv("AZURE_SEARCH_KEY"))
default_creds = az_credential if args.searchkey == None or args.storagekey == None else None
search_creds = search_key if args.searchkey == None else AzureKeyCredential(args.searchkey)
if not args.skipblobs:
    storage_creds = default_creds if args.storagekey == None else args.storagekey
if not args.localpdfparser:
    if formrecognizerservice == None:
        print("Error: Azure Form Recognizer service is not provided. Please provide formrecognizerservice or use --localpdfparser for local pypdf parser.")
        exit(1)
    formrecognizer_creds = default_creds if args.formrecognizerkey == None else AzureKeyCredential(args.formrecognizerkey)

# use default path if not provided as arguments
DEFAULT_FILES_PATH = "../data/*"
files_path = convert_to_realpath(DEFAULT_FILES_PATH) if args.files == None else args.files

if args.removeall:
    remove_blobs(None)
    remove_from_index(None)
else:
    if not args.remove:
        create_search_index(index)
    print("Processing files...")
    for filename in glob.glob(files_path):
        if args.verbose:
            print(f"Processing '{filename}'")
        if args.remove:
            remove_blobs(filename)
            remove_from_index(filename)
        elif args.removeall:
            remove_blobs(None)
            remove_from_index(None)
        else:
            procced_file = proccess_excel_file(filename)
            if not args.skipblobs:
                upload_blobs(procced_file)
            page_map = get_document_text(procced_file)
            sections = create_sections(os.path.basename(procced_file), page_map)
            index_sections(os.path.basename(procced_file), sections)
        move_file_to_temp(procced_file)
