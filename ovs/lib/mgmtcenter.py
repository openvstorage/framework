# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for MgmtCenterController
"""

from ovs.celery_run import celery
from ovs.dal.hybrids.mgmtcenter import MgmtCenter
from ovs.dal.hybrids.pmachine import PMachine
from ovs.extensions.hypervisor.factory import Factory
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('lib', name='mgmtcenter')


class MgmtCenterController(object):
    """
    Contains all BLL regarding MgmtCenters
    """

    @staticmethod
    @celery.task(name='ovs.mgmtcenter.test_connection')
    def test_connection(mgmt_center_guid):
        """
        Test management center connection
        """
        mgmt_center = MgmtCenter(mgmt_center_guid)
        try:
            mgmt_center_client = Factory.get_mgmtcenter(mgmt_center=mgmt_center)
        except Exception as ex:
            logger.error('Cannot get mgmt center client: {0}'.format(ex))
            return None
        try:
            is_mgmt_center = mgmt_center_client.test_connection()
        except Exception as ex:
            logger.error('Cannot test connection: {0}'.format(ex))
            return False
        return is_mgmt_center

    @staticmethod
    @celery.task(name='ovs.mgmtcenter.configure_host')
    def configure_host(pmachine_guid, mgmtcenter_guid, update_link):
        pmachine = PMachine(pmachine_guid)
        mgmt_center = MgmtCenter(mgmtcenter_guid)
        mgmt_center_client = None
        try:
            mgmt_center_client = Factory.get_mgmtcenter(mgmt_center=mgmt_center)
        except Exception as ex:
            logger.error('Cannot get management center client: {0}'.format(ex))
        if mgmt_center_client is not None:
            logger.info('Configuring host {0} on management center {1}'.format(pmachine.name, mgmt_center.name))
            mgmt_center_client.configure_host(pmachine.ip)
            if update_link is True:
                pmachine.mgmtcenter = mgmt_center
                pmachine.save()

    @staticmethod
    @celery.task(name='ovs.mgmtcenter.unconfigure_host')
    def unconfigure_host(pmachine_guid, mgmtcenter_guid, update_link):
        pmachine = PMachine(pmachine_guid)
        mgmt_center = MgmtCenter(mgmtcenter_guid)
        mgmt_center_client = None
        try:
            mgmt_center_client = Factory.get_mgmtcenter(mgmt_center=mgmt_center)
        except Exception as ex:
            logger.error('Cannot get management center client: {0}'.format(ex))
        if mgmt_center_client is not None:
            logger.info('Unconfiguring host {0} from management center {1}'.format(pmachine.name, mgmt_center.name))
            mgmt_center_client.unconfigure_host(pmachine.ip)
            if update_link is True:
                pmachine.mgmtcenter = None
                pmachine.save()

    @staticmethod
    @celery.task(name='ovs.mgmtcenter.is_host_configured')
    def is_host_configured(pmachine_guid):
        pmachine = PMachine(pmachine_guid)
        mgmt_center_client = None
        try:
            mgmt_center_client = Factory.get_mgmtcenter(pmachine=pmachine)
        except Exception as ex:
            if pmachine.mgmtcenter_guid:
                logger.error('Cannot get management center client: {0}'.format(ex))

        if mgmt_center_client is not None:
            return mgmt_center_client.is_host_configured(pmachine.ip)
        return False

    @staticmethod
    @celery.task(name='ovs.mgmtcenter.configure_vpool_for_host')
    def configure_vpool_for_host(pmachine_guid, vpool_guid):
        pmachine = PMachine(pmachine_guid)
        mgmt_center_client = None
        try:
            mgmt_center_client = Factory.get_mgmtcenter(pmachine=pmachine)
        except Exception as ex:
            logger.error('Cannot get management center client: {0}'.format(ex))
        if mgmt_center_client is not None:
            logger.info('Configuring vPool {0} on host {1}'.format(vpool_guid, pmachine.name))
            mgmt_center_client.configure_vpool_for_host(vpool_guid, pmachine.ip)

    @staticmethod
    @celery.task(name='ovs.mgmtcenter.unconfigure_vpool_for_host')
    def unconfigure_vpool_for_host(pmachine_guid, vpool_guid):
        pmachine = PMachine(pmachine_guid)
        mgmt_center_client = None
        try:
            mgmt_center_client = Factory.get_mgmtcenter(pmachine=pmachine)
        except Exception as ex:
            logger.error('Cannot get management center client: {0}'.format(ex))
        if mgmt_center_client is not None:
            logger.info('Unconfiguring vPool {0} on host {1}'.format(vpool_guid, pmachine.name))
            mgmt_center_client.unconfigure_vpool_for_host(vpool_guid, False, pmachine.ip)

    @staticmethod
    @celery.task(name='ovs.mgmtcenter.is_host_configured_for_vpool')
    def is_host_configured_for_vpool(pmachine_guid, vpool_guid):
        pmachine = PMachine(pmachine_guid)
        mgmt_center_client = None
        try:
            mgmt_center_client = Factory.get_mgmtcenter(pmachine=pmachine)
        except Exception as ex:
            if pmachine.mgmtcenter_guid:
                logger.error('Cannot get management center client: {0}'.format(ex))

        if mgmt_center_client is not None:
            return mgmt_center_client.is_host_configured_for_vpool(vpool_guid, pmachine.ip)
        return False
