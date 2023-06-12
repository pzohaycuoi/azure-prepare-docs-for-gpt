import json
import logging
from common import logger_config, log_function_call


logger_config()
logger = logging.getLogger("file")


@log_function_call
def fix_json(stuff: str):
    text = stuff
    text = text.replace('\n', '')
    text = text.strip()
    str_endwith = text[-1]
    json_chars = ['"', '{', '}', '[', ']', ',']
    if str_endwith not in json_chars:
        text += '"'
    else:
        if str_endwith == ',':
            if text[-2] == "}":
                text = text.rstrip(str_endwith)
            else:
                text += '"'

    # Find all brackets inside paragraph
    bracket_list = ['[', ']', '{', '}']
    all_brackets = []

    for char in text:
        if char in bracket_list:
            all_brackets.append(char)

    bracket_string = ''.join(all_brackets)
    while bracket_string.count('{}') != 0 or bracket_string.count('[]') != 0:
        bracket_string = bracket_string.replace('{}', '')
        bracket_string = bracket_string.replace('[]', '')

    missing_brackets = []
    for char in bracket_string:
        missing_brackets.append(char)

    missing_brackets.reverse()
    for bracket in missing_brackets:
        if bracket == '[':
            text += ']'
        if bracket == '{':
            text += '}'
            
    try:
        converted_text = json.loads(text)
    except Exception as e:
        logger.error(e)
        raise e

    return converted_text
