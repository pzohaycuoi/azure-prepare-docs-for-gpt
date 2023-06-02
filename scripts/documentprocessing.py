import html
import logging
from common import logger_config, log_function_call
from azure.ai.formrecognizer import AnalyzeResult
from azure.ai.formrecognizer._models import DocumentPage


logger_config()
logger = logging.getLogger("file")

class ProcessDocument():

    def __init__(self, form_recognizer_results: AnalyzeResult):
        self.form_recognizer_results = form_recognizer_results
        self.MAX_SECTION_LENGTH = 1000
        self.SENTENCE_SEARCH_LIMIT = 100
        self.SECTION_OVERLAP = 100
        self.SENTENCE_ENDINGS = [".", "!", "?"]
        self.WORDS_BREAKS = [",", ";", ":", " ", "(", ")", "[", "]", "{", "}", "\t", "\n"]

    @log_function_call
    def _table_to_html(self, table):
        """
        Converts a table object into an HTML table.

        This function takes a table object as input and converts it into an HTML table representation.
        It iterates through the cells of the table, grouping them by rows, and generates the HTML
        table structure. Each cell is assigned the appropriate HTML tag (th for column/header cells,
        td for data cells) based on its kind. The cell spans are also taken into account to set the
        corresponding colspan and rowspan attributes in the HTML table. The content of each cell is
        escaped to prevent HTML injection.

        Args:
            table (Table): The table object to convert.

        Returns:
            str: The HTML representation of the table.
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

    @log_function_call
    def _extract_page_text(self, page: DocumentPage):
        """
        Extracts the text from a page by concatenating the individual words.

        Args:
            page (PageResult): The page from which to extract the text.

        Returns:
            str: The extracted text from the page.
        """
        page_text = ""
        for line in page.lines:
            page_text += " ".join([word.content for word in line.get_words()]) + " "
        return page_text

    @log_function_call
    def _get_tables_on_page(self, all_tables, page_number):
        """
        Retrieves the tables on a specific page.

        Args:
            all_tables (list): All tables detected in the documfent.
            page_number (int): The page number for which to retrieve the tables.

        Returns:
            list: The tables on the specified page.
        """
        return [table for table in all_tables if table.bounding_regions[0].page_number == page_number]

    @log_function_call
    def _get_table_chars(self, text, tables):
        """
        Retrieves the character indices that correspond to table spans.

        Args:
            text (str): The text from the page.
            tables (list): The tables on the page.

        Returns:
            list: The table characters with their corresponding table ID.
        """
        table_chars = [-1] * len(text)
        for table_id, table in enumerate(tables):
            for span in table.spans:
                for i in range(span.offset, span.offset + span.length):
                    if 0 <= i < len(table_chars):
                        table_chars[i] = table_id
        return table_chars

    @log_function_call
    def _generate_page_text(self, text, table_chars, tables_on_page):
        """
        Generates the page text by replacing table spans with HTML representations.

        Args:
            text (str): The text from the page.
            table_chars (list): The table characters with their corresponding table ID.
            tables_on_page (list): The tables on the page.

        Returns:
            str: The generated page text with tables represented as HTML.
        """
        page_text = ""
        added_tables = set()
        for idx, char in enumerate(text):
            if table_chars[idx] == -1:
                page_text += char
            else:
                table_id = table_chars[idx]
                if table_id not in added_tables:
                    page_text += self._table_to_html(tables_on_page[table_id])
                    added_tables.add(table_id)
        return page_text

    @log_function_call
    def _get_document_text(self):
        """
        Retrieves the text and tables from the document analyzed by the Form Recognizer service.

        This function takes the results of the Form Recognizer analysis as input and extracts the
        text and tables from each page of the document. It iterates through the pages and extracts
        the text by concatenating the individual words. It also extracts the tables by identifying
        the table spans on each page and adding them to the page text. The table spans are replaced
        with their corresponding HTML representation using the _table_to_html function. The function
        builds a page map that stores the page number, offset, and page text for each page. Finally,
        it returns the page map.

        Args:
            form_recognizer_results (AnalyzeResult): The results of the Form Recognizer analysis.

        Returns:
            list: A list of tuples representing the page map. Each tuple contains the page number,
            offset, and page text.
        """
        self.page_map = []
        offset = 0

        for page_num, page in enumerate(self.form_recognizer_results.pages):
            page_text = self._extract_page_text(page)
            self.page_map.append((page_num, offset, page_text))
            offset += len(page_text)

            tables_on_page = self._get_tables_on_page(self.form_recognizer_results.tables, page_num + 1)
            table_chars = self._get_table_chars(page.spans[0].text, tables_on_page)

            page_text = self._generate_page_text(page.spans[0].text, table_chars, tables_on_page)
            self.page_map.append((page_num, offset, page_text))
            offset += len(page_text)

        return self.page_map

    @log_function_call
    def _find_page(self, offset):
        """
        Finds the page number for a given offset in the page map.

        Args:
            offset (int): The offset for which to find the page number.

        Returns:
            int: The page number.
        """
        for i, (page_num, page_offset, _) in enumerate(self.page_map):
            if offset >= page_offset and offset < self.page_map[i + 1][1]:
                return page_num
        return self.page_map[-1][0]

    @log_function_call
    def _find_sentence_end(self, text, end, length):
        """
        Finds the end of the sentence or a whole word boundary within the section limit.

        Args:
            text (str): The text to search.
            end (int): The current end position.
            length (int): The length of the text.

        Returns:
            int: The updated end position.
        """
        last_word = -1
        while end < length and (end - self.MAX_SECTION_LENGTH) < self.SENTENCE_SEARCH_LIMIT and text[end] not in self.SENTENCE_ENDINGS:
            if text[end] in self.WORDS_BREAKS:
                last_word = end
            end += 1
        if end < length and text[end] not in self.SENTENCE_ENDINGS and last_word > 0:
            end = last_word  # Fall back to at least keeping a whole word
        end = min(end + 1, length)  # Adjust the end position
        return end

    @log_function_call
    def _find_sentence_start(self, text, start, end):
        """
        Finds the start of the sentence or a whole word boundary within the section limit.

        Args:
            text (str): The text to search.
            start (int): The current start position.
            end (int): The current end position.

        Returns:
            int: The updated start position.
        """
        last_word = -1
        while start > 0 and (end - start - self.MAX_SECTION_LENGTH) < self.SENTENCE_SEARCH_LIMIT and text[start] not in self.SENTENCE_ENDINGS:
            if text[start] in self.WORDS_BREAKS:
                last_word = start
            start -= 1
        if start > 0 and text[start] not in self.SENTENCE_ENDINGS and last_word > 0:
            start = last_word
        start = max(start + 1, 0)  # Adjust the start position
        return start

    @log_function_call
    def split_text(self):
        """
        Splits the text from the page map into sections based on specified parameters.

        Returns:
            list: A list of tuples containing the section text and its corresponding page number.
        """
        self._get_document_text()
        all_text = "".join(p[2] for p in self.page_map)
        length = len(all_text)
        start = 0
        self.sections = []

        while start + self.SECTION_OVERLAP < length:
            end = min(start + self.MAX_SECTION_LENGTH, length)

            end = self.find_sentence_end(all_text, end, length)
            start = self.find_sentence_start(all_text, start, end)

            section_text = all_text[start:end]
            self.sections.append((section_text, self.find_page(start)))

            last_table_start = section_text.rfind("<table")
            if last_table_start > 2 * self.SENTENCE_SEARCH_LIMIT and last_table_start > section_text.rfind("</table"):
                start = min(end - self.SECTION_OVERLAP, start + last_table_start)
            else:
                start = end - self.SECTION_OVERLAP

        if start + self.SECTION_OVERLAP < end:
            self.sections.append((all_text[start:end], self.find_page(start)))

        return self.sections
