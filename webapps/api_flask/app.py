import os
from flask import Flask
from .response import ResponseOVS
# Blueprint
from backend.views import storagerouter_view

class OVSFlask(Flask):
    """
    Extensions of the standard Flask app to handle serializing of the OVS DAL objects
    """
    response_class = ResponseOVS

def create_app(test_config=None):
    # create and configure the app
    app = OVSFlask(__name__)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE='pyrakoon',
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    return app

app = create_app()
app.register_blueprint(storagerouter_view)