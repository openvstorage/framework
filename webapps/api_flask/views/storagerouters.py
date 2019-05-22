from flask import Blueprint
from ovs.dal.lists.storagerouterlist import StorageRouterList

url_prefix = 'storagerouters'

storagerouter_view = Blueprint(url_prefix, __name__)


@storagerouter_view.route('/{0}/'.format(url_prefix))
def list():
    """
    Overview of all StorageRouters
    :return: List of StorageRouters
    :rtype: list[ovs.dal.hybrids.storagerouter.StorageRouter]
    """
    return StorageRouterList.get_storagerouters()

