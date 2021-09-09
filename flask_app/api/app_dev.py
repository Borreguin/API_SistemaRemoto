""" Setting the production environment variable """
import os
os.environ['production_env'] = 'False'

import flask_app.settings.initial_settings as init
from flask_app.api.app import build_app
from flask_app.api import log


def main():
    # build the flask app (web application)
    app = build_app()
    init.DEBUG = True
    # serve the application in development mode
    log.info(f'>>>>> Starting development server <<<<<')
    app.run(debug=init.DEBUG, port=5000)
    log.info(f'>>>>> host: localhost port: {init.API_PORT}<<<<<')
    log.info(f">>>>> API running over: {init.API_PREFIX}")


if __name__ == "__main__":
    main()
