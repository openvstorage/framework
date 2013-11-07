from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.group import Group


class RoleGroup(DataObject):
    _blueprint = {}
    _relations = {'role': (Role, 'rolegroups'),
                  'group': (Group, 'rolegroups')}
    _expiry = {}