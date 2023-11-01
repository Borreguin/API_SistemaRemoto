from flask_app.my_lib.PI_connection.PIServer.PIServerBase import PIServerBase


class PIPointBase:
    def __init__(self, server: PIServerBase, tag_name: str):
        self.server = server
        self.tag_name = tag_name
        self.pt = server.find_PI_point(tag_name)

    def interpolated(self, time_range, span, as_df=True, numeric=True):
        pass

    def plot_values(self, time_range, n_samples, as_df=True, numeric=True):
        pass

    def recorded_values(self, time_range, AFBoundary, filterExpression=None, as_df=True, numeric=True):
        pass

    def summaries(self, time_range, span, AFSummaryTypes, AFCalculationBasis, AFTimestampCalculation):
        pass

    def filtered_summaries(self, time_range, summary_duration, filter_expression, summary_type, calc_basis,
                           sample_type, sample_interval, time_type):
        pass

    def time_filter(self, time_range, expression, span, time_unit="se"):
        pass

    def interpolated_value(self, timestamp):
        pass

    def snapshot(self):
        pass

    def current_value(self):
        pass

    def average(self, time_range, span):
        pass

    def max(self, time_range, span):
        pass

    def min(self, time_range, span):
        pass
