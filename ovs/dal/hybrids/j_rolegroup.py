# license see http://www.openvstorage.com/licenses/opensource/
"""
RoleGroup module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.group import Group


class RoleGroup(DataObject):
    """
    The RoleGroup class represents the junction table between Role and Group.
    """
    # pylint: disable=line-too-long
    _blueprint = {}
    _relations = {'role':  (Role,  'groups'),
                  'group': (Group, 'roles')}
    _expiry = {}
    # pylint: enable=line-too-long
