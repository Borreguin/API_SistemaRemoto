import os
import sys

# To include the project path in the Operating System path:
from pi_connect import create_pi_server, create_pi_point
from pi_util import create_time_range, create_span

script_path = os.path.dirname(os.path.abspath(__file__))
pi_connect_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(">>>>>\tAdding project path...", pi_connect_path)
sys.path.append(pi_connect_path)


def main():
    # import matplotlib.pyplot as plt
    pi_svr = create_pi_server()

    tag_name = "CAL_DIST_QUITO_P.CARGA_TOT_1_CAL.AV"
    pt = create_pi_point(pi_svr, tag_name)

    time_range = create_time_range("2018-02-12", "2018-02-14")
    time_range2 = create_time_range("2019-09-01", "2019-09-30")
    span = create_span("1h 30m")

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
    span = create_span("60m")
    df_max = pt.max(time_range, span)
    print(df_average)
    print(df_max)
    df_average.plot(title="Average")
    df_max.plot(title="Max values")
    # plt.show()

    pt.filtered_summaries(time_range2, summary_duration=AFTimeSpan.Parse("1d"),
                          # filter_expression= "'POMASQUI230JAMON_2_P.LINEA_RDV.AV' > 30",
                          filter_expression="'TADAY230MOLIN_1_IT.LIN52-2152_IE4.EQ' = \"TE\"",
                          summary_type=AFSummaryTypes.Count,
                          calc_basis=AFCalculationBasis.TimeWeighted,
                          sample_type=AFSampleType.ExpressionRecordedValues,
                          sample_interval=AFTimeSpan.Parse("15m"),
                          time_type=AFTimestampCalculation.Auto)

    print("end of script")


if __name__ == "__main__":
    main()

# piServers = PIServers()
# piServer = piServers.DefaultPIServer
# tag_name = "CAL_DIST_QUITO_P.CARGA_TOT_1_CAL.AV"
# tag_name = "SNI_GENERACION_P.TOTAL_CAL.AV"
# pt2 = PIPoint.FindPIPoint(piServer, tag_name)
# summaries = pt2.Summaries(timerange, span, AFSummaryTypes.Average,
# AFCalculationBasis.TimeWeighted, AFTimestampCalculation.Auto)
