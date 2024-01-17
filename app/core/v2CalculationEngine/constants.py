""" Constants for the calculation engine """

NUMBER_OF_PROCESSES = 4
TIMEOUT_SECONDS = 600 # 10 minutes

""" Time format """
yyyy_mm_dd = "%Y-%m-%d"
yyyy_mm_dd_hh_mm_ss = "%Y-%m-%d %H:%M:%S"

cl_success = "success"
cl_unavailability_minutes = "unavailability_minutes"
cl_message = "message"
cl_processed_time = "processed_time_minutes"
columns_unavailability = [cl_success, cl_unavailability_minutes, cl_processed_time, cl_message]
