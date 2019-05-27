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
This module contains generic hybrid serializers
"""
from flask import Response, current_app, request, json
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObject


def to_json(content, *args, **kwargs):
    """
    Converts content to json while respecting config options.
    """
    # @todo parse options regarding extra params

    indent = None
    separators = (',', ':')
    if isinstance(content, DataList):
        base_response = {u'data': [u'{0}'.format(o.serialize(**kwargs)['guid']) for o in content]}
        out = base_response
    elif isinstance(content, DataObject):
        out = content.serialize(**kwargs)
    else:
        raise RuntimeError('proper warning') #todo check for other types
    if (current_app.config['JSONIFY_PRETTYPRINT_REGULAR'] and not request.is_xhr):
        indent = 2
        separators = (', ', ': ')
    return json.dumps(out, indent=indent, separators=separators)
