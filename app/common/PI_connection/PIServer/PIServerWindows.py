from app.common.PI_connection import *
from app.common.PI_connection.PIServer.PIServerBase import PIServerBase
import pandas as pd
import numpy as np


class PIServerWindows(PIServerBase):
    def __init__(self, name=None):
        # if PIServer is not found:
        self.server = None
        if name is None:
            piServers = PIServers()
            self.server = piServers.DefaultPIServer
        else:
            piServers = list(PIServers().GetPIServers())
            for piServer in piServers:
                if name == piServer.Name:
                    self.server = piServer
        if simulation:
            print()

    def find_PI_point(self, tag_name: str):
        """
        Find a PI_point in PIServer
        https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/T_OSIsoft_AF_PI_PIPoint.htm
        :param tag_name: name of the tag
        :return: PIpoint
        """
        pt = None
        try:
            pt = PIPoint.FindPIPoint(self.server, tag_name)
        except Exception:
            pass
            # print("[pi_connect] [{0}] not found".format(tag_name))
        return pt

    def find_PI_point_list(self, list_tag_name: list) -> list:
        """
        Find a list of PI_point in PIServer
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
        :param numeric: force numeric value
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
