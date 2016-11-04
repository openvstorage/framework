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
Branding module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class Branding(DataObject):
    """
    The Branding class represents the specific OEM information.
    """
    __properties = [Property('name', str, unique=True, doc='Name of the Brand.'),
                    Property('description', str, mandatory=False, doc='Description of the Brand.'),
                    Property('css', str, doc='CSS file used by the Brand.'),
                    Property('productname', str, doc='Commercial product name.'),
                    Property('is_default', bool, doc='Indicates whether this Brand is the default one.')]
    __relations = []
    __dynamics = []
