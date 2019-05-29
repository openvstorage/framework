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

from flask import Response, request
from ovs.dal.dataobject import DataObject
from ovs.dal.datalist import DataList
from api_flask.backend.serializers.serializers import to_json
CONVERTABLE_TYPES = (list, dict, DataList, DataObject)


class ResponseOVS(Response):
    """
    Extend flask.Response with support for list/dict/OVS dal conversion to JSON.
    """

    def __init__(self, content=None, *args, **kwargs):
        if isinstance(content, CONVERTABLE_TYPES):
            kwargs['mimetype'] = 'application/json'
            extra_arguments = request.args
            content = to_json(content, **extra_arguments)

        super(Response, self).__init__(content, *args, **kwargs)

    @classmethod
    def force_type(cls, response, environ=None):
        """Override with support for list/dict."""
        if isinstance(response, CONVERTABLE_TYPES):
            return cls(response)
        else:
            return super(Response, cls).force_type(response, environ)
