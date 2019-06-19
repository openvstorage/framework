from abc import abstractmethod
from ovs.extensions.generic.system import System
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs_extensions.update.alba_component_update import AlbaComponentUpdater as _albacomponent_updater


class AlbaComponentUpdater(_albacomponent_updater):
    """
    Implementation of abstract class to update alba
    """

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
        # type: () -> str
        """
        use a factory to provide the machine id
        :return:
        """
        return System.get_my_machine_id()
