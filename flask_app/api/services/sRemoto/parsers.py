from flask_restplus import reqparse
from flask_app.my_lib import utils as u
import werkzeug
from flask_app.settings.initial_settings import SUPPORTED_FORMAT_DATES as fmt_time_list
from flask_app.settings.initial_settings import DEFAULT_DATE_FORMAT as fmt_time_default
import datetime as dt
"""
    Configure the API HTML to show for each services the arguments that are needed 
    (Explain the arguments for each service)
    Cada Parse indica como se deben obervar los modelos desde afuera, explicación 
    de la API en su página inicial
"""

excel_upload = reqparse.RequestParser()
excel_upload.add_argument('excel_file',
                         type=werkzeug.datastructures.FileStorage,
                         location='files',
                         required=True,
                         help='xls, xlsx file')
excel_upload_w_option = excel_upload
excel_upload_w_option.add_argument('option', type=str, required=False, help="'REEMPLAZAR' para sustituir por un nuevo nodo")

file_upload = reqparse.RequestParser()
file_upload.add_argument('file',
                         type=werkzeug.datastructures.FileStorage,
                         location='files',
                         required=True,
                         help='xls, xlsx file')


""" parser for data range"""
time_range = reqparse.RequestParser()
time_range.add_argument('ini-date', type=u.valid_date, help="formato: YYYY-MM-DD")
time_range.add_argument('end-date', type=u.valid_date, help="formato: YYYY-MM-DD")

""" arguments for test_dict service """
test = reqparse.RequestParser()
test.add_argument('test_id', type=str, required=True, default="any_index", help='String index')

""" arguments for tag service """
tag = reqparse.RequestParser()
tag.add_argument('tag_name', type=str, required=True, default="New_Tag_Name",
                 help='Check whether a TagPoint exists or not in the sRemoto')

tag.add_argument('tag_type', type=str, required=False, default="Generic",
                 help='Collection where the time series will be saved')


""" arguments for snapshoot """
format_time = reqparse.RequestParser()
format_time.add_argument("format_time", type=str, required=False, default="%Y-%m-%d %H:%M:%S",
                         help="Format: epoch or " + str(fmt_time_list))

""" arguments for recorded values """
start_time = dt.datetime.now() - dt.timedelta(days=1)
end_time = dt.datetime.now()
range_time = reqparse.RequestParser()
range_time.add_argument("start_time", type=str, required=False, default=start_time.strftime(fmt_time_default),
                        help="Supported formats: " + str(fmt_time_list))
range_time.add_argument("end_time", type=str, required=False, default=end_time.strftime(fmt_time_default),
                        help="Supported formats: " + str(fmt_time_list))
range_time.add_argument("format_time", type=str, required=False, default="%Y-%m-%d %H:%M:%S",
                        help="Format: " + str(fmt_time_list))


""" arguments for interpolated values """
range_time_with_span = range_time.copy()
range_time_with_span.add_argument("span", type=str, help="./help/span",
                                  required=False, default="15 min")

""" arguments for interpolated values with method """
range_time_with_span_and_method = range_time_with_span.copy()
range_time_with_span_and_method.add_argument("method", type=str, help="./help/interpolated/method",
                                             required=False, default="time")

""" arguments for registers service """
tag_list = reqparse.RequestParser()
tag_list.add_argument("id_utr_list", type=list, help="List of tag_names",
                      required=True, default=["dev1.tag1", "dev2.tag2", "dev3.tag3"])

"""arguments for id_utr_list with time format"""
tag_list_w_time_format = tag_list.copy()
tag_list_w_time_format.add_argument("format_time", type=str, required=False, default="%Y-%m-%d %H:%M:%S",
                        help="Format: " + str(fmt_time_list))

""" arguments for id_utr_list, date_range and time_format """
tag_list_time_range_w_time_format = tag_list_w_time_format.copy()
tag_list_time_range_w_time_format.add_argument("start_time", type=str, required=False,
                        default=start_time.strftime(fmt_time_default),
                        help="Supported formats: " + str(fmt_time_list))
tag_list_time_range_w_time_format.add_argument("end_time", type=str, required=False,
                        default=end_time.strftime(fmt_time_default),
                        help="Supported formats: " + str(fmt_time_list))
