from api.backend.serializers.serializers import FullSerializer
from celery.task.control import revoke
from flask import Blueprint
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.j_storagerouterdomain import StorageRouterDomain
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs_extensions.api.exceptions import HttpNotAcceptableException
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.disk import DiskController
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.generic import GenericController
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.update import UpdateController
from ovs.lib.vdisk import VDiskController
from ovs.lib.vpool import VPoolController
from api_flask.decorators import log
from api_flask.decorators import required_roles

url_prefix = '/storagerouters'

view = Blueprint(url_prefix, __name__, url_prefix=url_prefix)

DOMAIN_CHANGE_KEY = 'ovs_dedupe_domain_change'


@log()
@required_roles(['read', 'manage'])
@view.route('/')
def list():
    """
    Overview of all StorageRouters
    :return: List of StorageRouters
    :rtype: list[ovs.dal.hybrids.storagerouter.StorageRouter]
    """
    return StorageRouterList.get_storagerouters()


@log()
@required_roles(['read', 'manage'])
@view.route('/<storagerouter_guid>')
def retrieve(storagerouter_guid):
    """
    Load information about a given StorageRouter
    :param storagerouter_guid: StorageRouter guid
    :type storagerouter_guid: str
    :return: The StorageRouter requested
    :rtype: ovs.dal.hybrids.storagerouter.StorageRouter
    """
    return StorageRouter(storagerouter_guid)

@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/partial_update/') #todo fix
def partial_update(storagerouter_guid, request, contents=None):
    """
    Update a StorageRouter
    :param storagerouter_guid: StorageRouter to update
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :param request: The raw Request
    :type request: Request
    :param contents: Contents to be updated/returned
    :type contents: str
    :return: The StorageRouter updated
    :rtype: ovs.dal.hybrids.storagerouter.StorageRouter
    """
    contents = None if contents is None else contents.split(',')
    sr = StorageRouter(storagerouter_guid)
    serializer = FullSerializer(StorageRouter, contents=contents, instance=sr, data=request.DATA)  # todo fix
    storagerouter = serializer.deserialize()
    storagerouter.save()
    return storagerouter


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/mark_offline/')
def mark_offline(storagerouter_guid):
    """
    Marks all StorageDrivers of a given node offline. DO NOT USE ON RUNNING STORAGEROUTERS!
    :param storagerouter_guid: StorageRouter to mark offline
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return StorageDriverController.mark_offline.delay(storagerouter_guid)


@log()
@required_roles(['read'])
@view.route('/<storagerouter_guid>/get_metadata/')
def get_metadata(storagerouter_guid):
    """
    Returns a list of mount points on the given StorageRouter
    :param storagerouter_guid: StorageRouter to get the metadata from
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return StorageRouterController.get_metadata.delay(storagerouter_guid)


@log()
@required_roles(['read'])
@view.route('/<storagerouter_guid>/version_info/')
def get_version_info(storagerouter_guid):
    """
    DEPRECATED API CALL
    Gets version information of a given StorageRouter
    :param storagerouter_guid: StorageRouter to get the versions from
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return StorageRouterController.get_version_info.delay(storagerouter_guid)


@log()
@required_roles(['read', 'manage'])
@view.route('/<storagerouter_guid>/version_info/')
def get_support_info(storagerouter_guid):
    """
    Returns support information for the entire cluster
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter_guid
    return StorageRouterController.get_support_info.delay()


@log()
@required_roles(['read', 'manage'])
@view.route('/<storagerouter_guid>/get_proxy_config/<vpool_guid>')
def get_proxy_config(storagerouter_guid, vpool_guid):
    """
    Gets the ALBA proxy for a given StorageRouter and vPool
    :param storagerouter_guid: StorageRouter on which the ALBA proxy is configured
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :param vpool_guid: Guid of the vPool for which the proxy is configured
    :type vpool_guid: str
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return StorageRouterController.get_proxy_config.delay(vpool_guid=vpool_guid,
                                                          storagerouter_guid=storagerouter_guid)


@log()
@required_roles(['read', 'manage'])
@view.route('/<storagerouter_guid>/create_hprm_config_files/<parameters>')
def create_hprm_config_files(storagerouter_guid, storagerouter, parameters):
    """
    DEPRECATED API CALL - USE /vpool/vpool_guid/create_hprm_config_files instead
    Create the required configuration files to be able to make use of HPRM (aka PRACC)
    These configuration will be zipped and made available for download
    :param storagerouter_guid: StorageRouter this call is executed on
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :param storagerouter: The StorageRouter for which a HPRM manager needs to be deployed
    :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
    :param parameters: Additional information required for the HPRM configuration files
    :type parameters: dict
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter
    ExtensionsToolbox.verify_required_params(actual_params=parameters, required_params={'vpool_guid': (str, ExtensionsToolbox.regex_guid)})
    return VPoolController.create_hprm_config_files.delay(parameters=parameters,
                                                          vpool_guid=parameters['vpool_guid'],
                                                          local_storagerouter_guid=storagerouter_guid)


@log()
@required_roles(['read'])
@view.route('/<storagerouter_guid>/get_support_metadata/')
def get_support_metadata(storagerouter_guid):
    """
    Gets support metadata of a given StorageRouter
    :param storagerouter_guid: StorageRouter to get the support metadata from
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return StorageRouterController.get_support_metadata.apply_async(
        routing_key='sr.{0}'.format(StorageRouter(storagerouter_guid).machine_id)
    )


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/configure_support/<support_info>')
def configure_support(storagerouter_guid, support_info):
    """
    Configures support on all StorageRouters
    :param storagerouter_guid: guid of the sr
    :type storagerouter_guid: str
    :param support_info: Information about which components should be configured
        {'stats_monkey': True,  # Enable/disable the stats monkey scheduled task
         'support_agent': True,  # Responsible for enabling the ovs-support-agent service, which collects heart beat data
         'remote_access': False,  # Cannot be True when support agent is False. Is responsible for opening an OpenVPN tunnel to allow for remote access
         'stats_monkey_config': {}}  # Dict with information on how to configure the stats monkey (Only required when enabling the stats monkey
    :type support_info: dict
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter_guid
    return StorageRouterController.configure_support.delay(support_info=support_info)


@log()
@required_roles(['read', 'manage'])
@view.route('/<storagerouter_guid>/get_logfiles/<target_storagerouter_guid>')
def get_logfiles(storagerouter_guid, target_storagerouter_guid):
    """
    Collects logs, moves them to a web-accessible location and returns log TGZs filename
    :param storagerouter_guid: StorageRouter this call is executed on (to store the log files on)
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :param target_storagerouter_guid: The StorageRouter to collect the logs from
    :type target_storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """

    return StorageRouterController.get_logfiles.s(storagerouter_guid).apply_async(
        routing_key='sr.{0}'.format(StorageRouter(target_storagerouter_guid).machine_id)
    )


@log()
@required_roles(['read'])
@view.route('/<storagerouter_guid>/check_mtpt/<name>')
def check_mtpt(storagerouter_guid, name):
    """
    Validates whether the mount point for a vPool is available
    :param storagerouter_guid: The StorageRouter to validate the mount point on
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :param name: The name of the mount point to validate (vPool name)
    :type name: str
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return StorageRouterController.mountpoint_exists.delay(name=str(name), storagerouter_guid=storagerouter_guid)


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/add_vpool/<name>')
def add_vpool(call_parameters, storagerouter_guid, request):  #todo fix params
    """
    Adds a vPool to a given StorageRouter
    :param call_parameters: A complex (JSON encoded) dictionary containing all various parameters to create the vPool
    :type call_parameters: dict
    :param local_storagerouter: StorageRouter on which the call is executed
    :type local_storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
    :param request: The raw request
    :type request: Request
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    def lacks_connection_info(_connection_info, check_none=False):
        if check_none is True and _connection_info is None:
            return True
        else:
            return 'host' not in _connection_info or _connection_info['host'] in ['', None]

    def get_default_connection_info(_client, _connection_info):
        _connection_info['client_id'] = _client.client_id
        _connection_info['client_secret'] = _client.client_secret
        _connection_info['host'] = local_storagerouter.ip
        _connection_info['port'] = 443
        _connection_info['local'] = True
        return _connection_info

    local_storagerouter = StorageRouter(storagerouter_guid)

    # API backwards compatibility
    if 'backend_connection_info' in call_parameters:
        raise HttpNotAcceptableException(error='invalid_data',
                                         error_description='Invalid data passed: "backend_connection_info" is deprecated')

    # API client translation (cover "local backend" selection in GUI)
    if 'backend_info' not in call_parameters or 'connection_info' not in call_parameters or 'config_params' not in call_parameters:
        raise HttpNotAcceptableException(error='invalid_data',
                                         error_description='Invalid call_parameters passed')
    connection_info = call_parameters['connection_info']
    if 'backend_info_aa' in call_parameters:
        # Backwards compatibility
        call_parameters['backend_info_fc'] = call_parameters.pop('backend_info_aa')
    if 'connection_info_aa' in call_parameters:
        # Backwards compatibility
        call_parameters['connection_info_fc'] = call_parameters.pop('connection_info_aa')
    connection_info_fc = call_parameters.get('connection_info_fc')
    connection_info_bc = call_parameters.get('connection_info_bc')
    # Keeping '' for backwards compatibility
    if lacks_connection_info(connection_info) or lacks_connection_info(connection_info_fc, True) or lacks_connection_info(connection_info_bc, True):
        client = None
        for _client in request.client.user.clients:
            if _client.ovs_type == 'INTERNAL' and _client.grant_type == 'CLIENT_CREDENTIALS':  # todo fix
                client = _client
        if client is None:
            raise HttpNotAcceptableException(error='invalid_data',
                                             error_description='Invalid call_parameters passed')
        if lacks_connection_info(connection_info):
            connection_info = get_default_connection_info(client, connection_info)
            call_parameters['connection_info'] = connection_info
        if connection_info_fc is not None and lacks_connection_info(connection_info_fc):
            connection_info_fc = get_default_connection_info(client, connection_info_fc)
            call_parameters['connection_info_fc'] = connection_info_fc
        if connection_info_bc is not None and lacks_connection_info(connection_info_bc):
            connection_info_bc = get_default_connection_info(client, connection_info_bc)
            call_parameters['connection_info_bc'] = connection_info_bc

    if 'caching_info' not in call_parameters:
        call_parameters['caching_info'] = {'cache_quota_bc': call_parameters.pop('cache_quota_bc', None),
                                           'cache_quota_fc': call_parameters.pop('cache_quota_fc', None),
                                           'block_cache_on_read': call_parameters.pop('block_cache_on_read', False),
                                           'block_cache_on_write': call_parameters.pop('block_cache_on_write', False),
                                           'fragment_cache_on_read': call_parameters.pop('fragment_cache_on_read', False),
                                           'fragment_cache_on_write': call_parameters.pop('fragment_cache_on_write', False)}

    call_parameters.pop('type', None)
    call_parameters.pop('readcache_size', None)
    call_parameters['config_params'].pop('dedupe_mode', None)
    call_parameters['config_params'].pop('cache_strategy', None)

    # Finally, launching the add_vpool task
    return VPoolController.add_vpool.delay(VPoolController, call_parameters)


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/get_update_metadata/')
def get_update_metadata(storagerouter_guid):
    """
    Returns metadata required for updating
      - Checks if 'at' can be used properly
      - Checks if ongoing updates are busy
    :param storagerouter_guid: StorageRouter to get the update metadata from
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return UpdateController.get_update_metadata.delay(StorageRouter(storagerouter_guid).ip)


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/update_framework/')
def update_framework(storagerouter_guid):
    """
    Initiate a task on the given StorageRouter to update the framework on ALL StorageRouters
    DEPRECATED API call - use update_components in the future
    :param storagerouter_guid: StorageRouter to start the update on
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter_guid
    return UpdateController.update_components.delay(components=['framework'])


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/update_volumedriver/')
def update_volumedriver(storagerouter_guid):
    """
    Initiate a task on the given StorageRouter to update the volumedriver on ALL StorageRouters
    DEPRECATED API call - use update_components in the future
    :param storagerouter_guid: StorageRouter to start the update on
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter_guid
    return UpdateController.update_components.delay(components=['storagedriver'])


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/update_components/<components>')
def update_components(storagerouter_guid, components):
    """
    Initiate a task on a StorageRouter to update the specified components on ALL StorageRouters
    :param storagerouter_guid: StorageRouter to start the update on
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :param components: Components to update
    :type components: list
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter_guid
    return UpdateController.update_components.delay(components=components)


@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/configure_disk/<disk_guid>/<offset>/<size>/<roles>/<partition_guid>')
def configure_disk(storagerouter_guid, disk_guid, offset, size, roles, partition_guid=None):
    """
    Configures a disk on a StorageRouter
    :param storagerouter_guid: StorageRouter on which to configure the disk
    :type storagerouter_guid: str
    :param disk_guid: The GUID of the Disk to configure
    :type disk_guid: str
    :param offset: The offset of the partition to configure
    :type offset: int
    :param size: The size of the partition to configure
    :type size: int
    :param roles: A list of all roles to be assigned
    :type roles: list
    :param partition_guid: The guid of the partition if applicable
    :type partition_guid: str
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return StorageRouterController.configure_disk.delay(storagerouter_guid, disk_guid, partition_guid, offset, size, roles)


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/rescan_disks/')
def rescan_disks(storagerouter_guid):
    """
    Triggers a disk sync on the given StorageRouter
    :param storagerouter_guid: StorageRouter on which to rescan all disks
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return DiskController.sync_with_reality.delay(storagerouter_guid)


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/refresh_hardware/')
def refresh_hardware(storagerouter_guid):
    """
    Refreshes all hardware parameters
    :param storagerouter_guid: StorageRouter on which to refresh all hardware capabilities
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    return StorageRouterController.refresh_hardware.delay(storagerouter_guid)


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/set_domains/<domain_guids>/<recovery_domain_guids>')
def set_domains(storagerouter_guid, domain_guids, recovery_domain_guids):
    """
    Configures the given domains to the StorageRouter.
    :param storagerouter_guid: The StorageRouter to update
    :type storagerouter_guid: ovs.dal.hybrids.storagerouter.StorageRouter
    :param domain_guids: A list of Domain guids
    :type domain_guids: list
    :param recovery_domain_guids: A list of Domain guids to set as recovery Domain
    :type recovery_domain_guids: list
    :return: None
    :rtype: None
    """
    change = False
    storagerouter = StorageRouter(storagerouter_guid)
    for junction in storagerouter.domains:
        if junction.backup is False:
            if junction.domain_guid not in domain_guids:
                junction.delete()
                change = True
            else:
                domain_guids.remove(junction.domain_guid)
        else:
            if junction.domain_guid not in recovery_domain_guids:
                junction.delete()
                change = True
            else:
                recovery_domain_guids.remove(junction.domain_guid)
    for domain_guid in domain_guids + recovery_domain_guids:
        junction = StorageRouterDomain()
        junction.domain = Domain(domain_guid)
        junction.backup = domain_guid in recovery_domain_guids
        junction.storagerouter = storagerouter
        junction.save()
        change = True

    # Schedule a task to run after 60 seconds, re-schedule task if another identical task gets triggered
    if change is True:
        cache = VolatileFactory.get_client()
        task_ids = cache.get(DOMAIN_CHANGE_KEY)
        if task_ids:
            for task_id in task_ids:
                revoke(task_id)
        task_ids = [MDSServiceController.mds_checkup.s().apply_async(countdown=60).id,
                    VDiskController.dtl_checkup.s().apply_async(countdown=60).id,
                    StorageDriverController.cluster_registry_checkup.s().apply_async(countdown=60).id]
        cache.set(DOMAIN_CHANGE_KEY, task_ids, 600)  # Store the task ids
        storagerouter.invalidate_dynamics(['regular_domains', 'recovery_domains'])


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/merge_package_information/')
def merge_package_information(storagerouter_guid):
    """
    Retrieve the package information from the model for both StorageRouters and ALBA Nodes and merge it
    :param storagerouter_guid: The StorageRouter to update
    :type storagerouter_guid: str
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter_guid
    return UpdateController.merge_package_information.delay()


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/refresh_package_information/')
def refresh_package_information(storagerouter_guid):
    """
    Refresh the updates for all StorageRouters
    :param storagerouter_guid: The StorageRouter to update
    :type storagerouter_guid: str
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter_guid
    return GenericController.refresh_package_information.delay()


@log()
@required_roles(['read', 'write', 'manage'])
@view.route('/<storagerouter_guid>/get_update_information/')
def get_update_information(storagerouter_guid):
    """
    :param storagerouter_guid: The StorageRouter to update
    :type storagerouter_guid: str
    Retrieve the update information for all StorageRouters
    This contains information about
        - downtime of model, GUI, vPools, proxies, ...
        - services that will be restarted
        - packages that will be updated
        - prerequisites that have not been met
    :return: Asynchronous result of a CeleryTask
    :rtype: celery.result.AsyncResult
    """
    _ = storagerouter_guid
    return UpdateController.merge_downtime_information.delay()
