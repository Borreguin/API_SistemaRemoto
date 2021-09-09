import threading
import datetime as dt


def get_thread_by_name(name):
    threads = threading.enumerate()
    for thread in threads:
        if thread.getName() == name:
            return thread
    return None


def get_today():
    today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return today


def get_period(today=None):
    if today is None:
        today = get_today()
    if today.day > 1:
        return today - dt.timedelta(days=today.day - 1), today
    else:
        yesterday = today - dt.timedelta(days=1)
        return yesterday - dt.timedelta(days=yesterday.day - 1), today