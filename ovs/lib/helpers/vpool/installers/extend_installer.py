from ovs.lib.helpers.vpool.installers.installer import VPoolInstaller


class ExtendInstaller(object, VPoolInstaller):

    def __init__(self, name):
        super(ExtendInstaller, self).__init__(name)
        self.is_new = False

    def validate(self, storagerouter=None, storagedriver=None):
        super(ExtendInstaller, self).validate(storagerouter, storagedriver)

        if storagerouter:
            for vpool_storagedriver in self.vpool.storagedrivers:
                if vpool_storagedriver.storagerouter_guid == storagerouter.guid:
                    raise RuntimeError('A StorageDriver is already linked to this StorageRouter for vPool {0}'.format(self.vpool.name))
