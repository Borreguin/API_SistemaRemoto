
import datetime as dt
from typing import List


class DateTimeRange:
    start : dt.datetime
    end : dt.datetime

    def __init__(self, start: dt.datetime, end: dt.datetime):
        self.start = start
        self.end = end

    def __str__(self):
        return f"DateTimeRange: {self.start} - {self.end}"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.start == other.start and self.end == other.end

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.start, self.end))

    def __lt__(self, other):
        return self.start < other.start

    def __le__(self, other):
        return self.start <= other.start

    def __gt__(self, other):
        return self.start > other.start

    def __ge__(self, other):
        return self.start >= other.start

    def __contains__(self, other):
        return self.start <= other.start and self.end >= other.end

    def get_time_in_minutes(self):
        t_delta = self.end - self.start
        time_in_minutes = t_delta.days * (60 * 24) + t_delta.seconds // 60 + (t_delta.seconds % 60) / 60
        return time_in_minutes

def get_total_time_in_minutes(time_range_list: List[DateTimeRange]):
    return sum([t.get_time_in_minutes() for t in time_range_list])

