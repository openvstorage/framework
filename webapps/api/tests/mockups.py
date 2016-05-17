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
Mockups module
"""


class FullSerializer(object):
    """
    Dummy class
    """

    def __init__(self, object_type, contents, instance, many=False):
        """
        Dummy initializer
        """
        self.data = {'object_type': object_type.__name__,
                     'contents': contents,
                     'instance': instance,
                     'many': many}


class Serializers(object):
    """
    Dummy class
    """

    FullSerializer = FullSerializer

    def __init__(self):
        """
        Dummy initializer
        """
        pass
