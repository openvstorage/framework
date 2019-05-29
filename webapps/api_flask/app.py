# Copyright (C) 2019 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

import os
from flask import Flask
from .response import ResponseOVS
# Blueprint
from backend.views import storagerouter_view
from backend.views import vdisks_view

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
app.register_blueprint(vdisks_view)