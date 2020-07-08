""" coding: utf-8
Created by rsanchez on 03/05/2018
Este proyecto ha sido desarrollado en la Gerencia de Operaciones de CENACE
Mateo633
"""

import pandas as pd
import sys
import clr
import datetime
import numpy as np

sys.path.append(r'C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0')
clr.AddReference('OSIsoft.AFSDK')

from OSIsoft.AF import *
from OSIsoft.AF.PI import *
from OSIsoft.AF.Asset import *
from OSIsoft.AF.Data import *
from OSIsoft.AF.Time import *
from OSIsoft.AF.UnitsOfMeasure import *


class PIserver:
    def __init__(self, name=None):
        # if PIserver is not found:
        self.server = None
        if name is None:
            piServers = PIServers()
            self.server = piServers.DefaultPIServer
        else:
            piServers = list(PIServers().GetPIServers())
            for piServer in piServers:
                if name == piServer.Name:
                    self.server = piServer


    def find_PI_point(self, tag_name: str):
        """
        Find a PI_point in PIserver
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/T_OSIsoft_AF_PI_PIPoint.htm
        :param tag_name: name of the tag
        :return: PIpoint
        """
        pt = None
        try:
            pt = PIPoint.FindPIPoint(self.server, tag_name)
        except Exception as e:
            pass
            # print("[pi_connect] [{0}] not found".format(tag_name))
        return pt

    def find_PI_point_list(self, list_tag_name: list) -> list:
        """
        Find a list of PI_point in PIserver
        :param list_tag_name: name of the tag
        :return: PIpoint list
        """
        assert isinstance(list_tag_name, list)
        pi_point_list = list()
        for tag in list_tag_name:
            pi_point = PI_point(self, tag_name=tag)
            pi_point_list.append(pi_point)
        return pi_point_list

    def interpolated_of_tag_list(self, tag_list, time_range, span, numeric=False):
        """
        Return a DataFrame that contains the values of each tag in column
        and the timestamp as index
        :param tag_list: list of tags
        :param time_range: PIServer.time_range [AFTimeRange]
        :param span: PIServer.span
        :return: DataFrame
        """
        pi_points = list()
        for tag in tag_list:
            pi_points.append(PI_point(self, tag))

        df_result = pi_points[0].interpolated(time_range, span, as_df=True, numeric=numeric)

        for piPoint in pi_points[1:]:
            df_result = pd.concat([df_result, piPoint.interpolated(time_range, span, numeric=numeric)], axis=1)

        return df_result

    def snapshot_of_tag_list(self, tag_list, time):

        df_result = pd.DataFrame(columns=tag_list, index=[str(time)])

        for tag in tag_list:
            try:
                pt = PI_point(self, tag)
                df_result[tag] = pt.interpolated_value(time)
            except Exception as e:
                print(e)
                df_result[str(tag)] = np.nan

        return df_result


class PI_point:

    def __init__(self, server: PIserver, tag_name: str):
        assert isinstance(server, PIserver)
        self.server = server
        self.tag_name = tag_name
        self.pt = server.find_PI_point(tag_name)

    def interpolated(self, time_range, span, as_df=True, numeric=True):
        """
        returns the interpolate values of a PIpoint
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/M_OSIsoft_AF_PI_PIPoint_InterpolatedValues.htm
        :param numeric: try to convert to numeric values [True, False]
        :param as_df: return as DataFrame  [True, False]
        :param time_range: PIServer.time_range [AFTimeRange]
        :param span: PIServer.span      [AFTimeSpan]
        :return: returns the interpolate values of a PIpoint
        """
        values = None
        try:
            values = self.pt.InterpolatedValues(time_range, span, "", False)
        except Exception as e:
            print(e)
            print("[pi_connect] [{0}, {1}] no correct object".format(time_range, span))
        if as_df:
            values = to_df(values, self.tag_name, numeric=numeric)
            mask = ~values.index.duplicated()
            values = values[mask]
        return values

    def plot_values(self, time_range, n_samples, as_df=True, numeric=True):
        """
        n_samples of the tag in time range
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/M_OSIsoft_AF_Asset_AFValues_PlotValues.htm
        The number of intervals to plot over. Typically, this would be the number of horizontal pixels in a horizontal trend.
        :param numeric: try to convert to numeric values
        :param as_df: return as DataFrame
        :param time_range:  PIServer.timerange
        :param n_samples:
        :return: OSIsoft.AF.Asset.AFValues
        """
        values = None
        try:
            values = self.pt.PlotValues(time_range, n_samples)
        except Exception as e:
            print(e)
            print("[pi_connect] [{0}, {1}] no correct object".format(time_range, n_samples))
        if as_df:
            values = to_df(values, self.tag_name, numeric)
        return values

    def recorded_values(self, time_range, AFBoundary=AFBoundaryType.Interpolated,
                        filterExpression=None,
                        as_df=True, numeric=True):
        """
        recorded values for a tag
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/M_OSIsoft_AF_PI_PIPoint_RecordedValues.htm
        :param time_range: PIServer.time_range [AFTimeRange]
        :param AFBoundary: AFBoundary [Inside, Outside, Interpolated]
        :param filterExpression: A filter expression that follows the performance equation syntax.
        Ex: "'TADAY230MOLIN_1_IT.LIN52-2152_IE4.EQ' = \"TE\""
        :param numeric: Convert to numeric [True, False]
        :param as_df: return as DataFrame [True, False]
        :return: OSIsoft.AF.Asset.AFValues
        """
        values = None
        if isinstance(AFBoundary, int):
            if not AFBoundary >= 0 and AFBoundary <= 3:
                AFBoundary = 0
        elif AFBoundary.upper() == "INSIDE":
            AFBoundary = AFBoundaryType.Inside
        elif AFBoundary.upper() == "OUTSIDE":
            AFBoundary = AFBoundaryType.Outside
        elif AFBoundary.upper() == "INTERPOLATED":
            AFBoundary = AFBoundaryType.Interpolated

        try:
            if filterExpression is None:
                values = self.pt.RecordedValues(time_range, AFBoundary, "", False)
            else:
                values = self.pt.RecordedValues(time_range, AFBoundary, filterExpression, False)
        except Exception as e:
            print(e)
            print("[pi_connect] [{0}, {1}] no correct object".format(time_range, AFBoundary))
        if as_df:
            values = to_df(values, self.tag_name, numeric)
        return values

    def summaries(self, time_range, span, AFSummaryTypes=AFSummaryTypes.Average,
                  AFCalculationBasis=AFCalculationBasis.TimeWeighted,
                  AFTimestampCalculation=AFTimestampCalculation.Auto):
        """
        Returns a list of summaries
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/M_OSIsoft_AF_PI_PIPoint_Summaries_1.htm
        :param time_range: PIServer.time_range
        :param span: PIServer.span
        :param AFSummaryTypes: [Total, Average, Minimum, Maximum, etc]
        :param AFCalculationBasis: [TimeWeighted, EventWeighted, TimeWeightedContinuous]
        :param AFTimestampCalculation: [Auto, EarliestTime, MostRecentTime]
        :return: Returns a list of summaries
        """
        values = None
        try:
            values = self.pt.Summaries(time_range, span, AFSummaryTypes,
                                       AFCalculationBasis,
                                       AFTimestampCalculation)
        except Exception as e:
            print(e)
            print("[pi_connect] [{0}, {1}, {2}] no correct object".format(time_range, span, AFSummaryTypes))

        return values

    def filtered_summaries(self, time_range, summary_duration=AFTimeSpan.Parse("15m"),
                           # filter_expression= "'UTR_ADELCA_IEC8705101.SV' = 'INDISPONIBLE'",
                           filter_expression="'POMASQUI230JAMON_2_P.LINEA_RDV.AV' > 31",
                           summary_type=AFSummaryTypes.Total,
                           calc_basis=AFCalculationBasis.TimeWeighted,
                           sample_type=AFSampleType.ExpressionRecordedValues,
                           sample_interval=AFTimeSpan.Parse("15m"),
                           time_type=AFTimestampCalculation.Auto):
        """
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/M_OSIsoft_AF_PI_PIPoint_FilteredSummaries.htm
        When supplied a filter expression that evaluates to true or false,
        evaluates it over the passed time range. For the time ranges where the expression evaluates to true,
        the method calculates the requested summaries on the source attribute
        :param time_range:  [AFTimeRange]
        :param summary_duration: The duration of each summary interval [AFTimeSpan]
        :param filter_expression: A filter expression that follows the performance equation syntax [string]
        :param summary_type: A flag which specifies one or more summaries to compute for each interval
        over the time range [AFSummaryTypes]
        :param calc_basis: Specifies the method of evaluating the data over the time range [AFCalculationBasis]
        :param sample_type: Together with the sampleInterval, specifies how and how often
        the filter expression is evaluated. [AFSampleType] [ExpressionRecordedValues ( is evaluated at each of
        the timestamps of these retrieved events), Interval (only evaluated in each interval)]
        :param sample_interval: When the sampleType is Interval, it specifies how often the filter expression
        is evaluated when computing the summary for an interval. [AFTimeSpan]
        :param time_type: An enumeration value that specifies how the time stamp is calculated. [AFTimestampCalculation]
        (Auto, EarliestTime, MostRecentTime)
        :return: [DataFrame]
        """
        values = None
        try:
            values = self.pt.FilteredSummaries(time_range, summary_duration,
                                               filter_expression, summary_type,
                                               calc_basis, sample_type,
                                               sample_interval, time_type)
        except Exception as e:
            print(e)
            print("[pi_connect] [{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}] no correct object".format(time_range,
                                                                                                   summary_duration,
                                                                                                   filter_expression,
                                                                                                   summary_type,
                                                                                                   calc_basis,
                                                                                                   sample_type,
                                                                                                   sample_interval,
                                                                                                   time_type))

        df = pd.DataFrame()
        for summary in values:
            df = to_df(summary.Value, tag=self.tag_name)
        return df

    def time_filter(self, time_range, expression, span=AFTimeSpan.Parse("1d"), time_unit="se"):
        """
        Returns a DataFrame with calculus of filter time where the condition (expression) is True
        :param time_range: [AFTimeRange]
        :param expression: "'UTR_ADELCA_IEC8705101.SV' = 'INDISPONIBLE'" Ex: str
        :param span: [PIServer.span]
        :param time_unit: ["se" (segundos), "mi" (minutos), "ho" (horas), "di" (días)]
        :return:
        """
        if span is None:
            start, end = _datetime_start_and_time_of(time_range)
            t_delta = end - start
            minutes = t_delta.days * (60 * 24) + t_delta.seconds // 60 + t_delta.seconds % 60
            span = _span(str(minutes) + "m")

        value = self.filtered_summaries(time_range, summary_duration=span,
                                        filter_expression=expression,
                                        summary_type=AFSummaryTypes.Count,
                                        # cuenta el numero de segundos
                                        calc_basis=AFCalculationBasis.TimeWeighted,
                                        sample_type=AFSampleType.ExpressionRecordedValues,
                                        # con referencia al valor guardado (no interpolado)
                                        # sample_interval=AFTimeSpan.Parse("15m"),
                                        time_type=AFTimestampCalculation.Auto)
        # calculo en minutos
        if time_unit.upper() == "MI":
            value[self.tag_name] = value[self.tag_name] / 60

        if time_unit.upper() == "HO":
            value[self.tag_name] = value[self.tag_name] / 3600

        if time_unit.upper() == "DI":
            value[self.tag_name] = value[self.tag_name] / (3600 * 24)

        return value

    def interpolated_value(self, timestamp):
        """
        Gets interpolated value in timestamp
        :param timestamp: [str]
        :return: Value [Numeric, Status]
        """
        if isinstance(timestamp, datetime.datetime):
            timestamp = str(timestamp)

        try:
            time = AFTime(timestamp)
        except Exception as e:
            time = AFTime(str(datetime.datetime.now()))
            print(e)
            print("[pi_connect] [{0}] no correct format".format(timestamp))

        return self.pt.InterpolatedValue(time).Value

    def snapshot(self):
        if self.pt is None:
            return None
        return self.pt.Snapshot()

    def current_value(self):
        if self.pt is None:
            return None
        return self.pt.CurrentValue()

    def average(self, time_range, span):
        """
        Particular case of summaries function
        :param time_range: [AFRangeTime]
        :param span: [AFSpan]
        :return: DataFrame
        """
        summaries_list = self.summaries(time_range, span, AFSummaryTypes.Average)
        df = pd.DataFrame()
        for summary in summaries_list:
            df = to_df(summary.Value, tag=self.tag_name)
        return df

    def max(self, time_range, span):
        sumaries_list = self.summaries(time_range, span, AFSummaryTypes.Maximum)
        df = pd.DataFrame()
        for summary in sumaries_list:
            df = to_df(summary.Value, tag=self.tag_name)
        return df

    def min(self, time_range, span):
        sumaries_list = self.summaries(time_range, span, AFSummaryTypes.Minimum)
        df = pd.DataFrame()
        for summary in sumaries_list:
            df = to_df(summary.Value, tag=self.tag_name)
        return df


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


def _time_range(ini_time, end_time):
    """
    AFTimeRange:
    https://techsupport.osisoft.com/Documentation/PI-AF-SDK/Html/T_OSIsoft_AF_Time_AFTimeRange.htm
    :param ini_time: initial time (yyyy-mm-dd HH:MM:SS) [str, datetime]
    :param end_time: ending time (yyyy-mm-dd HH:MM:SS) [str, datetime]
    :return: AFTimeRange
    """
    timerange = None
    if isinstance(ini_time, datetime.datetime):
        ini_time = ini_time.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, datetime.datetime):
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")

    try:
        timerange = AFTimeRange(ini_time, end_time)
    except Exception as e:
        print(e)
        print("[pi_connect] [{0}, {1}] no correct format".format(ini_time, end_time))
    return timerange


def _time_range_for_today():
    """
    Time range of the current day from 0:00 to current time
    :return: AFTimeRange
    """
    dt = datetime.datetime.now()
    str_td = dt.strftime("%Y-%m-%d")
    return AFTimeRange(str_td, dt.strftime("%Y-%m-%d %H:%M:%S"))


def _time_range_for_today_all_day():
    """
    Time range of the current day from 0:00 to current time
    :return: AFTimeRange
    """
    dt = datetime.datetime.now()
    str_td = dt.strftime("%Y-%m-%d")
    dt_fin = dt.date() + datetime.timedelta(days=1)
    return AFTimeRange(str_td, dt_fin.strftime("%Y-%m-%d"))


def _start_and_end_time_of(time_range):
    """
    Gets the Start and End time of a AFTimeRange
    :param time_range:  AFTimeRange(str_ini_date, str_end_date)
    :return: Start and End time in format: yyyy-mm-dd HH:MM:SS [str, str]
    """
    assert isinstance(time_range, AFTimeRange)
    return time_range.StartTime.ToString("yyyy-MM-dd HH:mm:s"), time_range.EndTime.ToString("yyyy-MM-dd HH:mm:s")


def _datetime_start_and_time_of(time_range):
    """
    Gets the Start and End time of a AFTimerange as datetime
    :param time_range: AFTimeRange(str_ini_date, str_end_date)
    :return: as datetime object
    """
    start, end = _start_and_end_time_of(time_range)
    return datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S"), \
           datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")


def _span(delta_time):
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


def test():
    import matplotlib.pyplot as plt
    pi_svr = PIserver()

    tag_name = "CAL_DIST_QUITO_P.CARGA_TOT_1_CAL.AV"
    pt = PI_point(pi_svr, tag_name)

    time_range = _time_range("2018-02-12", "2018-02-14")
    time_range2 = _time_range("2019-09-01", "2019-09-30")
    span = _span("1h 30m")

    df1 = pt.interpolated(time_range, span)
    df2 = pt.plot_values(time_range, 200)
    df_raw = pt.recorded_values(time_range)
    [print(df) for df in (df1, df2, df_raw)]
    value1 = pt.snapshot()
    print("value1:" + str(value1))
    value2 = pt.current_value()
    print("value2:" + str(value2))
    df1.plot(title="Interporlada cada " + str(span))
    df2.plot(title="Metodo plot values")
    df_raw.plot(title="Recorded values")

    tag_list = ['JAMONDIN230POMAS_1_P.LINEA_ICC.AV', 'POMASQUI230JAMON_1_P.LINEA_RDV.AV',
                'POMASQUI230JAMON_2_P.LINEA_RDV.AV',
                'JAMONDIN230POMAS_1_P.LINEA_ICC.AQ', 'POMASQUI230JAMON_1_P.LINEA_RDV.AQ']
    df_all = pi_svr.interpolated_of_tag_list(tag_list, time_range, span)
    df_all.plot()
    # plt.show()

    df_average = pt.average(time_range, span)
    span = _span("60m")
    df_max = pt.max(time_range, span)
    print(df_average)
    print(df_max)
    df_average.plot(title="Average")
    df_max.plot(title="Max values")
    # plt.show()

    value = pt.filtered_summaries(time_range2, summary_duration=AFTimeSpan.Parse("1d"),
                                  # filter_expression= "'POMASQUI230JAMON_2_P.LINEA_RDV.AV' > 30",
                                  filter_expression="'TADAY230MOLIN_1_IT.LIN52-2152_IE4.EQ' = \"TE\"",
                                  summary_type=AFSummaryTypes.Count,
                                  calc_basis=AFCalculationBasis.TimeWeighted,
                                  sample_type=AFSampleType.ExpressionRecordedValues,
                                  sample_interval=AFTimeSpan.Parse("15m"),
                                  time_type=AFTimestampCalculation.Auto)

    print("end of script")


if __name__ == "__main__":
    perform_test = False
    if perform_test:
        test()

# piServers = PIServers()
# piServer = piServers.DefaultPIServer
# tag_name = "CAL_DIST_QUITO_P.CARGA_TOT_1_CAL.AV"
# tag_name = "SNI_GENERACION_P.TOTAL_CAL.AV"
# pt2 = PIPoint.FindPIPoint(piServer, tag_name)
# summaries = pt2.Summaries(timerange, span, AFSummaryTypes.Average,
# AFCalculationBasis.TimeWeighted, AFTimestampCalculation.Auto)
