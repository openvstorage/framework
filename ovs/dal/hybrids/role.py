# license see http://www.openvstorage.com/licenses/opensource/
"""
Role module
"""
from ovs.dal.dataobject import DataObject


class Role(DataObject):
    """
    The Role class represents a Role. A Role is used to allow execution of a certain set of
    actions. E.g. a "Viewer" Role can view all data but has no update/write permission.
    the data
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the Role'),
                  'code':        (None, str, 'Contains a code which is referenced from the API code'),
                  'description': (None, str, 'Description of the Role')}
    _relations = {}
    _expiry = {}
    # pylint: enable=line-too-long
