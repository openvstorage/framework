from ovs.lib.helpers.vpool.container import Container
from ovs.lib.vdisk import VDiskController
from ovs.lib.mdsservice import MDSServiceController


class ShrinkInstaller(object, Container):

    def __init__(self, name):
        super(ShrinkInstaller, self).__init__(name)
        self.is_new = False

    def validate(self, storagerouter=None, storagedriver=None):
        super(ShrinkInstaller, self).validate(storagerouter, storagedriver)

        if storagedriver is not None:
            VDiskController.sync_with_reality(vpool_guid=self.vpool.guid)
            storagedriver.invalidate_dynamics('vdisks_guids')
            if len(storagedriver.vdisks_guids) > 0:
                raise RuntimeError('There are still vDisks served from the given StorageDriver')

            self.mds_services = [mds_service for mds_service in self.vpool.mds_services if mds_service.service.storagerouter_guid == storagedriver.storagerouter_guid]
            for mds_service in self.mds_services:
                if len(mds_service.storagedriver_partitions) == 0 or mds_service.storagedriver_partitions[0].storagedriver is None:
                    raise RuntimeError('Failed to retrieve the linked StorageDriver to this MDS Service {0}'.format(mds_service.service.name))

    def remove_mds_services(self):
        """
        Remove the MDS services related to the StorageDriver being deleted
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        # Removing MDS services
        self._logger.info('Removing MDS services')
        errors_found = False
        for mds_service in self.mds_services:
            try:
                self._logger.info('Remove MDS service (number {0}) for StorageRouter with IP {1}'.format(mds_service.number, self.sr_installer.storagerouter.ip))
                MDSServiceController.remove_mds_service(mds_service=mds_service,
                                                        reconfigure=False,
                                                        allow_offline=self.sr_installer.root_client is None)  # No root_client means the StorageRouter is offline
            except Exception:
                self._logger.exception('Removing MDS service failed')
                errors_found = True
        return errors_found

    def update_node_distance_map(self):
        """
        Update the node distance map property for each StorageDriver when removing a StorageDriver
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        try:
            storagedriver = self.sd_installer.storagedriver
            for sd in self.vpool.storagedrivers:
                if sd != storagedriver:
                    sd.invalidate_dynamics('cluster_node_config')
                    config = sd.cluster_node_config
                    if storagedriver.storagedriver_id in config['node_distance_map']:
                        del config['node_distance_map'][storagedriver.storagedriver_id]
            return False
        except Exception:
            self._logger.exception('Failed to update the node_distance_map property')
            return True
