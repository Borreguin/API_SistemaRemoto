
import logging
import os
import sqlite3
from datetime import datetime


class SQLiteHandler(logging.Handler):
    # Inherit from logging.Handler
    """
    Logging handler that write logs to SQLite DB
    """
    def __init__(self, filename=None):
        global db
        if filename is None:
            script_path = os.path.dirname(os.path.abspath(__file__))
            db_path = script_path.replace("my_lib", "logs")
            filename = os.path.join(db_path, "logs.db")
        # run the regular Handler __init__
        logging.Handler.__init__(self)
        # Our custom argument
        db = sqlite3.connect(filename) # might need to use self.filename
        db.execute("CREATE TABLE IF NOT EXISTS debug(date datetime, loggername text, filename, srclineno integer, func text, level text, msg text)")
        db.commit()

    def emit(self, record):
        # record.message is the log message
        thisdate = datetime.now()
        db.execute(
            'INSERT INTO debug(date, loggername, filename, srclineno, func, level, msg) VALUES(?,?,?,?,?,?,?)',
            (
                thisdate,
                record.name,
                os.path.abspath(record.filename),
                record.lineno,
                record.funcName,
                record.levelname,
                record.msg,
            )
        )
        db.commit()


if __name__ == '__main__':

    script_path = os.path.dirname(os.path.abspath(__file__))
    db_path = script_path.replace("my_lib", "logs")
    db_file = os.path.join(db_path,"logs.db")
    # Create a logging object (after configuring logging)
    logger = logging.getLogger('DLogger')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(SQLiteHandler(db_file))
    logger.debug('Test 1')
    logger.warning('Some warning')
    logger.error('Alarma!')

