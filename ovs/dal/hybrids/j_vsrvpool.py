"""
VSRVpool module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter


class VSRVpool(DataObject):
    """
    The RoleGroup class represents the junction table between Role and Group
    """
    # pylint: disable=line-too-long
    _blueprint = {}
    _relations = {'vpool': (VPool,               'vsrs'),
                  'vsr':   (VolumeStorageRouter, 'vpools')}
    _expiry = {}
    # pylint: enable=line-too-long
