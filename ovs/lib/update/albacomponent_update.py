from abc import abstractmethod
from ovs_extensions.update.albacomponent_update import AlbaComponentUpdater as _albacomponent_updater
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.generic.system import System
from ovs.extensions.services.servicefactory import ServiceFactory

logger = logging.getLogger(__name__)


class AlbaComponentUpdater(_albacomponent_updater):
    """
    Implementation of abstract class to update alba
    """

    SERVICE_MANAGER = ServiceFactory.get_manager()

    @staticmethod
    @abstractmethod
    def get_persistent_client():
        # type: () -> PyrakoonStore
        """
        Retrieve a persistent client which needs
        Needs to be implemented by the callee
        """
        return PersistentFactory.get_client()

    @classmethod
    def get_node_id(cls):
        return System.get_my_machine_id()


if __name__ == '__main__':
    print AlbaComponentUpdater.restart_services()