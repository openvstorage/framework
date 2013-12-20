# license see http://www.openvstorage.com/licenses/opensource/
"""
BrandingList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObjectList
from ovs.dal.hybrids.branding import Branding


class BrandingList(object):
    """
    This BrandingList class contains various lists regarding to the Branding class
    """

    @staticmethod
    def get_brandings():
        """
        Returns a list of all brandings
        """
        brandings = DataList({'object': Branding,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': []}}).data
        return DataObjectList(brandings, Branding)
