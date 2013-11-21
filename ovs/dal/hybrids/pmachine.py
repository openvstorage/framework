"""
PMachine module
"""
from ovs.dal.dataobject import DataObject


class PMachine(DataObject):
    """
    The PMachine class represents a physical machine
    """
    _blueprint = {'name': (None, str),
                  'description': (None, str),
                  'hvtype': (None, ['HYPERV', 'VMWARE', 'XEN']),
                  'username': (None, str),
                  'password': (None, str),
                  'ip': (None, str)}
    _relations = {}
    _expiry = {}
