"""
RoleGroup module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.group import Group


class RoleGroup(DataObject):
    """
    The RoleGroup class represents the junction table between Role and Group
    """
    _blueprint = {}
    _relations = {'role': (Role, 'rolegroups'),
                  'group': (Group, 'rolegroups')}
    _expiry = {}
