import logging
import os
import sys
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from app import project_path

log_path = os.path.join(project_path, "logs")

# Logger configuration:
rotating_file_handler = {"maxBytes": 500000, "backupCount": 5, "mode": "a"}


def configure_logger(log_name: str = None, with_time: bool = True, level: logging = logging.INFO) -> logging.Logger:

    if log_name is None:
        log_name = "Default.log"
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    log_file_name = os.path.join(log_path, log_name)
    rotating_file_handler["filename"] = log_file_name
    logger = logging.getLogger(log_name)
    if with_time:
        formatter = logging.Formatter('%(levelname)s - [%(asctime)s] - %(message)s')
    else:
        formatter = logging.Formatter('%(levelname)s - %(message)s')

    # creating rotating and stream Handler
    r_handler = RotatingFileHandler(**rotating_file_handler)
    r_handler.setFormatter(formatter)
    s_handler = StreamHandler(sys.stdout)
    # adding handlers:
    logger.addHandler(r_handler)
    # logger.addHandler(s_handler)

    # setting logger in class
    logger.setLevel(level)
    return logger

