import os
from datetime import datetime
from logging import getLogger, FileHandler, StreamHandler, Formatter

logger = getLogger()

logger.setLevel("INFO")

stream_handler = StreamHandler()
stream_formatter = Formatter(
    "%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)
stream_handler.setFormatter(stream_formatter)
logger.addHandler(stream_handler)

LOG_FOLDER = "./logs/"
log_file_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
file_handler = FileHandler(os.path.join(LOG_FOLDER, log_file_name))
file_formatter = Formatter(
    "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)s] - %(message)s"
)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
