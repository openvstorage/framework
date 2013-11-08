from ovs.dal.dataobject import DataObject


class Branding(DataObject):
    _blueprint = {'name': (None, str),
                  'description': (None, str),
                  'css': (None, str),
                  'productname': (None, str),
                  'is_default': (False, bool)}
    _relations = {}
    _expiry = {}