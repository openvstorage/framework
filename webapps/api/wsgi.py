# Copyright (C) 2016 iNuron NV
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

"""
WSGI config for Open vStorage project.

This module contains the WSGI application used by Django's development server
and any production WSGI deployments. It should expose a module-level variable
named ``application``. Django's ``runserver`` and ``runfcgi`` commands discover
this application via the ``WSGI_APPLICATION`` setting.

Usually you will have the standard Django WSGI application here, but it also
might make sense to replace the whole Django WSGI application with a custom one
that later delegates to the Django one. For example, you could introduce WSGI
middleware here, or combine a Django application with an application of another
framework.

"""
import os
from ovs.extensions.log import configure_logging

# Configure OpenvStorage logging before initializing django as the settings
# API logging is configured in the settings
configure_logging()

# We defer to a DJANGO_SETTINGS_MODULE already in the environment.
# This breaks if running multiple sites in the same mod_wsgi process.
# To fix this, use mod_wsgi daemon mode with each site in its own daemon process,
# or use os.environ["DJANGO_SETTINGS_MODULE"] = "django.settings"
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.settings")

# This application object is used by any WSGI server configured to use this file.
# This includes Django's development server, if the WSGI_APPLICATION setting points here.
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

# Apply WSGI middleware here.
# from helloworld.wsgi import HelloWorldApplication
# application = HelloWorldApplication(application)
