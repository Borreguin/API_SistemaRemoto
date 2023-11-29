from app.common.PI_connection.PIServer.PIUtilBase import PIUtilBase
import pandas as pd
from datetime import datetime, timedelta

from app.common.PI_connection.pi_util import start_and_end_time_of


class PIUtilWindows(PIUtilBase):

    @staticmethod
    def to_df(values, tag, numeric=True):
        """
        returns a DataFrame based on PI values
        :param numeric: try to convert to numeric values
        :param values: PI values
        :param tag: name of the PI tag
        :return: DataFrame
        """
        df = pd.DataFrame()
        try:
            timestamp = [x.Timestamp.ToString("yyyy-MM-dd HH:mm:s") for x in values]
            df = pd.DataFrame(index=pd.to_datetime(timestamp))
            if numeric:
                df[tag] = pd.to_numeric([x.Value for x in values], errors='coerce')
            else:
                df[tag] = [x.Value for x in values]
        except Exception as e:
            print(e)
            print("[pi_connect] [{0}] to pdf".format(values))
        return df

    @staticmethod
    def create_time_range(ini_time, end_time):
        """
        AFTimeRange:
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/Html/T_OSIsoft_AF_Time_AFTimeRange.htm
        :param ini_time: initial time (yyyy-mm-dd HH:MM:SS) [str, datetime]
        :param end_time: ending time (yyyy-mm-dd HH:MM:SS) [str, datetime]
        :return: AFTimeRange
        """
        timerange = None
        if isinstance(ini_time, datetime):
            ini_time = ini_time.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(end_time, datetime):
            end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

        try:
            timerange = AFTimeRange(ini_time, end_time)
        except Exception as e:
            print(e)
            print("[pi_connect] [{0}, {1}] no correct format".format(ini_time, end_time))
        return timerange

    @staticmethod
    def time_range_for_today():
        """
        Time range of the current day from 0:00 to current time
        :return: AFTimeRange
        """
        dt = datetime.now()
        str_td = dt.strftime("%Y-%m-%d")
        return AFTimeRange(str_td, dt.strftime("%Y-%m-%d %H:%M:%S"))

    @staticmethod
    def time_range_for_today_all_day():
        """
        Time range of the current day from 0:00 to current time
        :return: AFTimeRange
        """
        dt = datetime.now()
        str_td = dt.strftime("%Y-%m-%d")
        dt_fin = dt.date() + timedelta(days=1)
        return AFTimeRange(str_td, dt_fin.strftime("%Y-%m-%d"))

    @staticmethod
    def start_and_end_time_of(time_range):
        """
        Gets the Start and End time of a AFTimeRange
        :param time_range:  AFTimeRange(str_ini_date, str_end_date)
        :return: Start and End time in format: yyyy-mm-dd HH:MM:SS [str, str]
        """
        assert isinstance(time_range, AFTimeRange)
        return time_range.StartTime.ToString("yyyy-MM-dd HH:mm:s"), time_range.EndTime.ToString("yyyy-MM-dd HH:mm:s")

    @staticmethod
    def datetime_start_and_time_of(time_range):
        """
        Gets the Start and End time of a AFTimerange as datetime
        :param time_range: AFTimeRange(str_ini_date, str_end_date)
        :return: as datetime object
        """
        start, end = start_and_end_time_of(time_range)
        return datetime.strptime(start, "%Y-%m-%d %H:%M:%S"), \
            datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

    @staticmethod
    def create_span(delta_time):
        """
        AFTimeSpan object
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/Html/Overload_OSIsoft_AF_Time_AFTimeSpan_Parse.htm
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/Html/M_OSIsoft_AF_Time_AFTimeSpan_Parse_1.htm
        :param delta_time: ex: "30m" [str] Format according the following regular expressions:
        [+|-]<number>[.<number>] <interval> { [+|-]<number>[.<number>] <interval> }* or
        [+|-]{ hh | [hh][:[mm][:ss[.ff]]] }
        :return: AFTimeSpan object
        """
        span = None
        try:
            span = AFTimeSpan.Parse(delta_time)
        except Exception as e:
            print(e)
            print("[pi_connect] [{0}] no correct format for span value".format(delta_time))
        return span