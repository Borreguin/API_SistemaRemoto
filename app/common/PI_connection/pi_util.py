from app.common.PI_connection import simulation
from app.common.PI_connection.PIServer.PIUtilSimulation import PIUtilSimulation


def to_df(values, tag, numeric=True):
    if simulation:
        return PIUtilSimulation.to_df(values, tag, numeric)
    else:
        from app.common.PI_connection.PIServer.PIUtilWindows import PIUtilWindows
        return PIUtilWindows.to_df(values, tag, numeric)

def create_time_range(ini_time, end_time):
    if simulation:
        return PIUtilSimulation.create_time_range(ini_time, end_time)
    else:
        from app.common.PI_connection.PIServer.PIUtilWindows import PIUtilWindows
        return PIUtilWindows.create_time_range(ini_time, end_time)


def time_range_for_today():
    if simulation:
        return PIUtilSimulation.time_range_for_today()
    else:
        from app.common.PI_connection.PIServer.PIUtilWindows import PIUtilWindows
        return PIUtilWindows.time_range_for_today()


def time_range_for_today_all_day():
    if simulation:
        return PIUtilSimulation.time_range_for_today_all_day()
    else:
        from app.common.PI_connection.PIServer.PIUtilWindows import PIUtilWindows
        return PIUtilWindows.time_range_for_today_all_day()


def start_and_end_time_of(time_range):
    if simulation:
        return PIUtilSimulation.start_and_end_time_of(time_range)
    else:
        from app.common.PI_connection.PIServer.PIUtilWindows import PIUtilWindows
        return PIUtilWindows.start_and_end_time_of(time_range)


def datetime_start_and_time_of(time_range):
    if simulation:
        return PIUtilSimulation.datetime_start_and_time_of(time_range)
    else:
        from app.common.PI_connection.PIServer.PIUtilWindows import PIUtilWindows
        return PIUtilWindows.datetime_start_and_time_of(time_range)


def create_span(delta_time):
    if simulation:
        return PIUtilSimulation.create_span(delta_time)
    else:
        from app.common.PI_connection.PIServer.PIUtilWindows import PIUtilWindows
        return PIUtilWindows.create_span(delta_time)