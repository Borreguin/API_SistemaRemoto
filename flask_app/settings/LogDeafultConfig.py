import logging
import os
import sys
from logging import StreamHandler
from logging.handlers import RotatingFileHandler

from flask_app.settings.initial_settings import log_path, ROTATING_FILE_HANDLER, ROTATING_FILE_HANDLER_LOG_LEVEL


class LogDefaultConfig:
    """
    Default configuration for the logger file:
    """
    rotating_file_handler = None

    def __init__(self, log_name: str = None, with_time=True):
        if log_name is None:
            log_name = "Default.log"

        self.log_file_name = os.path.join(log_path, log_name)
        self.rotating_file_handler = ROTATING_FILE_HANDLER
        self.rotating_file_handler["filename"] = self.log_file_name
        logger = logging.getLogger(log_name)
        if with_time:
            formatter = logging.Formatter('%(levelname)s - [%(asctime)s] - %(message)s')
        else:
            formatter = logging.Formatter('%(levelname)s - %(message)s')
        # creating rotating and stream Handler
        R_handler = RotatingFileHandler(**self.rotating_file_handler)
        R_handler.setFormatter(formatter)
        S_handler = StreamHandler(sys.stdout)
        # adding handlers:
        logger.addHandler(R_handler)
        logger.addHandler(S_handler)

        # setting logger in class
        self.logger = logger

        self.level = ROTATING_FILE_HANDLER_LOG_LEVEL["value"]
        options = ROTATING_FILE_HANDLER_LOG_LEVEL["options"]
        if self.level in options:
            if self.level == "error":
                self.logger.setLevel(logging.ERROR)
            if self.level == "warning":
                self.logger.setLevel(logging.WARNING)
            if self.level == "debug":
                self.logger.setLevel(logging.DEBUG)
            if self.level == "info":
                self.logger.setLevel(logging.INFO)
            if self.level == "off":
                self.logger.setLevel(logging.NOTSET)
        else:
            self.logger.setLevel(logging.ERROR)