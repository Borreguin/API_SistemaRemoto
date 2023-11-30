import sys, os
from datetime import datetime
import pandas as pd

from PIServer.PIPointBase import PIPointBase
from PIServer.PIServerBase import PIServerBase
from pi_util import to_df, datetime_start_and_time_of, create_span

pi_label = "PI-connect ->"
AF_PATH = os.path.abspath(os.path.join(os.sep, "Program Files (x86)", "PIPC", "AF", "PublicAssemblies", "4.0"))

# AF Client modules
sys.path.append(AF_PATH)
print(f"{pi_label} add AF path: {AF_PATH}")
import clr
clr.AddReference('OSIsoft.AFSDK')
from OSIsoft.AF import *
from OSIsoft.AF.PI import *
from OSIsoft.AF.Asset import *
from OSIsoft.AF.Data import *
from OSIsoft.AF.Time import *
from OSIsoft.AF.UnitsOfMeasure import *


class PIPointWindows(PIPointBase):

    def __init__(self, server: PIServerBase, tag_name: str):
        super().__init__(server, tag_name)
        assert isinstance(server, PIServerBase)
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

    def recorded_values(self, time_range, AFBoundary=None, filterExpression=None,
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
        if AFBoundary is None:
            AFBoundary = AFBoundaryType.Interpolated
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
            print(f"[pi_connect] [{time_range}, {summary_duration}, {filter_expression}, "
                  f"{summary_type}, {summary_type}, {calc_basis}, {sample_type}, {sample_interval} {time_type}] no correct object")

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
            start, end = datetime_start_and_time_of(time_range)
            t_delta = end - start
            minutes = t_delta.days * (60 * 24) + t_delta.seconds // 60 + (t_delta.seconds % 60) / 60
            span = create_span(str(minutes) + "m")

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
        if isinstance(timestamp, datetime):
            timestamp = str(timestamp)

        try:
            time = AFTime(timestamp)
        except Exception as e:
            time = AFTime(str(datetime.now()))
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

