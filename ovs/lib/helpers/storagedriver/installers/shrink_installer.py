from ovs.lib.helpers.storagedriver.container import Container
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.constants.vpools import HOSTS_PATH, PROXY_PATH


class ShrinkInstaller(Container):


    def clean_config_management(self):
        """
        Remove the configuration management entries related to a StorageDriver removal
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        try:
            for proxy in self.storagedriver.alba_proxies:
                Configuration.delete(PROXY_PATH.format(self.vp_container.vpool.guid), proxy.guid)
            Configuration.delete(HOSTS_PATH.format(self.vp_container.vpool.guid, self.storagedriver.storagedriver_id))
            return False
        except Exception:
            self._logger.exception('Cleaning configuration management failed')
            return True

    def clean_directories(self, mountpoints):
        """
        Remove the directories from the filesystem when removing a StorageDriver
        :param mountpoints: The mountpoints on the StorageRouter of the StorageDriver being removed
        :type mountpoints: list
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        self._logger.info('Deleting vPool related directories and files')
        dirs_to_remove = [self.storagedriver.mountpoint] + [sd_partition.path for sd_partition in self.storagedriver.partitions]
        try:
            for dir_name in dirs_to_remove:
                if dir_name and self.sr_installer.root_client.dir_exists(dir_name) and dir_name not in mountpoints and dir_name != '/':
                    self.sr_installer.root_client.dir_delete(dir_name)
            return False
        except Exception:
            self._logger.exception('StorageDriver {0} - Failed to retrieve mount point information or delete directories'.format(self.storagedriver.guid))
            self._logger.warning('StorageDriver {0} - Following directories should be checked why deletion was prevented: {1}'.format(self.storagedriver.guid, ', '.join(dirs_to_remove)))
            return True

    def clean_model(self):
        """
        Clean up the model after removing a StorageDriver
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        self._logger.info('Cleaning up model')
        try:
            for sd_partition in self.storagedriver.partitions[:]:
                sd_partition.delete()
            for proxy in self.storagedriver.alba_proxies:
                service = proxy.service
                proxy.delete()
                service.delete()

            sd_can_be_deleted = True
            if self.vp_container.storagedriver_amount <= 1:
                for relation in ['mds_services', 'storagedrivers', 'vdisks']:
                    expected_amount = 1 if relation == 'storagedrivers' else 0
                    if len(getattr(self.vp_container.vpool, relation)) > expected_amount:
                        sd_can_be_deleted = False
                        break

            if sd_can_be_deleted is True:
                self.storagedriver.delete()
            return False
        except Exception:
            self._logger.exception('Cleaning up the model failed')
            return True