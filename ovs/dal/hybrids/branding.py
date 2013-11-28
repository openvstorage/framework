# license see http://www.openvstorage.com/licenses/opensource/
"""
Branding module
"""
from ovs.dal.dataobject import DataObject


class Branding(DataObject):
    """
    The Branding class represents the specific OEM information.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None,  str,  'Name of the Brand.'),
                  'description': (None,  str,  'Description of the Brand.'),
                  'css':         (None,  str,  'CSS file used by the Brand.'),
                  'productname': (None,  str,  'Commercial product name.'),
                  'is_default':  (False, bool, 'Indicates whether this Brand is the default one.')}
    _relations = {}
    _expiry = {}
    # pylint: enable=line-too-long
