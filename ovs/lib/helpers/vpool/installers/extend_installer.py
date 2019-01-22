from ovs.lib.helpers.vpool.installers.installer import VPoolInstaller
from ovs.dal.hybrids.vpool import VPool


class ExtendInstaller(VPoolInstaller):

    def __init__(self, name):
        super(ExtendInstaller, self).__init__(name)
        self.is_new = False

    def validate(self, storagerouter=None, storagedriver=None):
        super(ExtendInstaller, self).validate(storagerouter, storagedriver)

        if storagerouter:
            for vpool_storagedriver in self.vpool.storagedrivers:
                if vpool_storagedriver.storagerouter_guid == storagerouter.guid:
                    raise RuntimeError('A StorageDriver is already linked to this StorageRouter for vPool {0}'.format(self.vpool.name))

    def ensure_exists(self, *args, **kwargs):
        """
        This
        :param args:
        :param kwargs:
        :return:
        """
        self.update_status(status=VPool.STATUSES.EXTENDING)

    def configure_cluster_registry(self, exclude=None, apply_on=None, allow_raise=False):
        """
        Retrieve the cluster node configurations for the StorageDrivers related to the vPool without the excluded StorageDrivers
        :param exclude: List of StorageDrivers to exclude from the node configurations
        :type exclude: list
        :param apply_on: Apply the updated cluster configurations on these StorageDrivers (or all but current if none provided)
        :type apply_on: list[ovs.dal.hybrids.storagedriver.StorageDriver]
        :param allow_raise: Allow the function to raise an exception instead of returning True when an exception occurred (Defaults to False)
        :type allow_raise: bool
        :raises Exception: When allow_raises is True and and updating the configuration would have failed
        :return: A boolean indication whether something failed
        :rtype: bool
        """
        try:
            super(ExtendInstaller, self).configure_cluster_registry(exclude, apply_on, allow_raise)
        except:
            self.revert_vpool(status=VPool.STATUSES.FAILURE)
