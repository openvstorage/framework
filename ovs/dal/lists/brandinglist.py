from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObjectList
from ovs.dal.hybrids.branding import Branding


class BrandingList(object):
    @staticmethod
    def get_brandings():
        brandings = DataList(key   = 'brandings',
                             query = {'object': Branding,
                                      'data': DataList.select.DESCRIPTOR,
                                      'query': {'type': DataList.where_operator.AND,
                                                'items': []}}).data
        return DataObjectList(brandings, Branding)