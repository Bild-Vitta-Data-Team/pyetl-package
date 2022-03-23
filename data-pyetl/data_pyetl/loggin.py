from asyncio.log import logger
import logging
import os
import sys
from pathlib import Path
from io import StringIO
from os import listdir
from os.path import isfile, join


def root_folder():
    """Returns the root path for the project"""

    script_folder = os.path.dirname(os.path.realpath(__file__))
    root = Path(script_folder).parent

    return root


def create_folders():
    """Create the folder directory for the project"""

    folders = ['logs', 'temp']
    for folder in folders:
        if os.path.isdir(f'{root_folder()}/{folder}') == False:
            os.mkdir(os.path.join(root_folder(), folder))


def set_logger():
    """Creates the logger"""

    create_folders()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(
        filename=f"{root_folder()}/logs/etl.log")
    stdout_handler = logging.StreamHandler(sys.stdout)
    handlers = [file_handler, stdout_handler]

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
        handlers=handlers,
    )
    return logger
