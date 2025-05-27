import os
from logging import getLogger, StreamHandler, Formatter
from logging.handlers import RotatingFileHandler


LOG_FOLDER = "./logs/"
os.makedirs(LOG_FOLDER, exist_ok=True)

logger = getLogger()

logger.setLevel("INFO")

stream_handler = StreamHandler()
stream_formatter = Formatter(
    "%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)
stream_handler.setFormatter(stream_formatter)
logger.addHandler(stream_handler)

file_handler = RotatingFileHandler(
    os.path.join(LOG_FOLDER, "app.log"),
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=5,
    encoding="utf-8"
)
file_formatter = Formatter(
    "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)s] - %(message)s"
)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
