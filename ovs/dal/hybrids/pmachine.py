"""
PMachine module
"""
from ovs.dal.dataobject import DataObject


class PMachine(DataObject):
    """
    The PMachine class represents a physical machine
    """
    _blueprint = {'name': (None, str, 'Name of the physical machine'),
                  'description': (None, str, 'Description of the physical machine'),
                  'hvtype': (None, ['HYPERV', 'VMWARE', 'XEN'], 'Hypervisor type'),
                  'username': (None, str, 'Username of the physical machine'),
                  'password': (None, str, 'Password of the physical machine'),
                  'ip': (None, str, 'IP address of the physical machine')}
    _relations = {}
    _expiry = {}
