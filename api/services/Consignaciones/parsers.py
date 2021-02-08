from flask_restplus import reqparse
from my_lib import utils as u
import werkzeug
from settings.initial_settings import SUPPORTED_FORMAT_DATES as fmt_time_list
from settings.initial_settings import DEFAULT_DATE_FORMAT as fmt_time_default
import datetime as dt

"""
    Configure the API HTML to show for each services the arguments that are needed 
    (Explain the arguments for each service)
    Cada Parse indica como se deben obervar los modelos desde afuera, explicación 
    de la API en su página inicial
"""

consignacion_upload = reqparse.RequestParser()
consignacion_upload.add_argument('file',
                                 type=werkzeug.datastructures.FileStorage,
                         location='files',
                         required=False,
                         help='pdf, xls, xlsx file')


