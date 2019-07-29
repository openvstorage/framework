## Tasks
### Disk
#### sync_with_reality
```
Syncs the Disks from the StorageRouter specified with the reality.
CHANGES MADE TO THIS CODE SHOULD BE REFLECTED IN THE ASD-MANAGER list_disks CALL TOO!
:param storagerouter_guid: Guid of the Storage Router to synchronize
:type storagerouter_guid: str
:return: None
```
### Generic
#### collapse_arakoon
```
Collapse Arakoon's Tlogs
:return: None
```
#### delete_snapshots
```
Delete snapshots based on the retention policy
Offloads concurrency to celery
Returns a GroupResult. Waiting for the result can be done using result.get()
:param timestamp: Timestamp to determine whether snapshots should be kept or not, if none provided, current time will be used
:type timestamp: float
:return: The GroupResult
:rtype: GroupResult
```
#### delete_snapshots_storagedriver
```
Delete snapshots per storagedriver & scrubbing policy

Implemented delete snapshot policy:
< 1d | 1d bucket | 1 | best of bucket   | 1d
< 1w | 1d bucket | 6 | oldest of bucket | 7d = 1w
< 1m | 1w bucket | 3 | oldest of bucket | 4w = 1m
> 1m | delete

:param storagedriver_guid: Guid of the StorageDriver to remove snapshots on
:type storagedriver_guid: str
:param timestamp: Timestamp to determine whether snapshots should be kept or not, if none provided, current time will be used
:type timestamp: float
:param group_id: ID of the group task. Used to identify which snapshot deletes were called during the scheduled task
:type group_id: str
:return: None
```
#### execute_scrub
```
Divide the scrub work among all StorageRouters with a SCRUB partition
:param vpool_guids: Guids of the vPools that need to be scrubbed completely
:type vpool_guids: list
:param vdisk_guids: Guids of the vDisks that need to be scrubbed
:type vdisk_guids: list
:param storagerouter_guid: Guid of the StorageRouter to execute the scrub work on
:type storagerouter_guid: str
:param manual: Indicator whether the execute_scrub is called manually or as scheduled task (automatically)
:type manual: bool
:return: None
:rtype: NoneType
```
#### refresh_package_information
```
Retrieve and store the package information of all StorageRouters
:return: None
```
#### run_backend_domain_hooks
```
Run hooks when the Backend Domains have been updated
:param backend_guid: Guid of the Backend to update
:type backend_guid: str
:return: None
```
#### snapshot_all_vdisks
```
Snapshots all vDisks
```
### Mds
#### ensure_safety
```
Ensures (or tries to ensure) the safety of a given vDisk.
Assumptions:
    * A local overloaded master is better than a non-local non-overloaded master
    * Prefer master/slaves to be on different hosts, a subsequent slave on the same node doesn't add safety
    * Don't actively overload services (e.g. configure an MDS as slave causing it to get overloaded)
    * Too much safety is not wanted (it adds loads to nodes while not required)
    * Order of slaves is:
        * All slaves on StorageRouters in primary Domain of vDisk host
        * All slaves on StorageRouters in secondary Domain of vDisk host
        * Eg: Safety of 2 (1 master + 1 slave)
            mds config = [local master in primary, slave in secondary]
        * Eg: Safety of 3 (1 master + 2 slaves)
            mds config = [local master in primary, slave in primary, slave in secondary]
        * Eg: Safety of 4 (1 master + 3 slaves)
            mds config = [local master in primary, slave in primary, slave in secondary, slave in secondary]
:param vdisk_guid: vDisk GUID to calculate a new safety for
:type vdisk_guid: str
:param excluded_storagerouter_guids: GUIDs of StorageRouters to leave out of calculation (Eg: When 1 is down or unavailable)
:type excluded_storagerouter_guids: list[str]
:raises RuntimeError: If host of vDisk is part of the excluded StorageRouters
                      If host of vDisk is not part of the StorageRouters in the primary domain
                      If catchup command fails for a slave
                      If MDS client cannot be created for any of the current or new MDS services
                      If updateMetadataBackendConfig would fail for whatever reason
:raises SRCObjectNotFoundException: If vDisk does not have a StorageRouter GUID
:return: None
:rtype: NoneType
```
#### ensure_safety_vpool
```
Ensures safety for a single vdisk of a vpool
Allows multiple ensure safeties to run at the same time for different vpool
Used internally
:param vpool_guid: Guid of the VPool associated with the vDisk
:type vpool_guid: str
:param vdisk_guid: Guid of the vDisk to the safety off
:type vdisk_guid: str
:param excluded_storagerouter_guids: GUIDs of StorageRouters to leave out of calculation (Eg: When 1 is down or unavailable)
:type excluded_storagerouter_guids: list[str]
:return: None
:rtype: NoneType
```
#### mds_catchup
```
Looks to catch up all MDS slaves which are too far behind
Only one catch for every storagedriver is invoked
```
#### mds_checkup
```
Validates the current MDS setup/configuration and takes actions where required
Actions:
    * Verify which StorageRouters are available
    * Make mapping between vPools and its StorageRouters
    * For each vPool make sure every StorageRouter has at least 1 MDS service with capacity available
    * For each vPool retrieve the optimal configuration and store it for each StorageDriver
    * For each vPool run an ensure safety for all vDisks
:raises RuntimeError: When ensure safety fails for any vDisk
:return: None
:rtype: NoneType
```
#### mds_checkup_single
```
Validates the current MDS setup/configuration and takes actions where required
Actions:
    * Verify which StorageRouters are available
    * Make mapping between vPools and its StorageRouters
    * For each vPool make sure every StorageRouter has at least 1 MDS service with capacity available
    * For each vPool retrieve the optimal configuration and store it for each StorageDriver
    * For each vPool run an ensure safety for all vDisks
:param vpool_guid: Guid of the VPool to do the checkup for
:type vpool_guid: str
:param mds_dict: OrderedDict containing all mds related information
:type mds_dict: collections.OrderedDict
:param offline_nodes: Nodes that are marked as unreachable
:type offline_nodes: List[StorageRouter]
:raises RuntimeError: When ensure safety fails for any vDisk
:return: None
:rtype: NoneType
:raises: MDSCheckupEnsureSafetyFailures when the ensure safety has failed for any vdisk
```
### Migration
#### migrate
```
Executes async migrations. It doesn't matter too much when they are executed, as long as they get eventually
executed. This code will typically contain:
* "dangerous" migration code (it needs certain running services)
* Migration code depending on a cluster-wide state
* ...
* Successfully finishing a piece of migration code, should create an entry in /ovs/framework/migration in case it should not be executed again
*     Eg: /ovs/framework/migration|stats_monkey_integration: True
```
### Monitoring
#### verify_vdisk_cache_quota
```
Validates whether the caching quota is reaching its limits or has surpassed it
Each vDisk can consume a part of the total fragment caching capacity
```
### Stats_monkey
#### run_all
```
Run all the get stats methods from StatsMonkeyController
Prerequisites when adding content:
    * New methods which need to be picked up by this method need to start with 'get_stats_'
    * New methods need to collect the information and return a bool and list of stats. Then 'run_all_get_stat_methods' method, will send the stats to the configured instance (influx / redis)
    * The frequency each method needs to be executed can be configured via the configuration management by setting the function name as key and the interval in seconds as value
    *    Eg: {'get_stats_mds': 20}  --> Every 20 seconds, the MDS statistics will be checked upon
```
### Storagedriver
#### cluster_registry_checkup
```
Verify whether changes have occurred in the cluster registry for each vPool
:return: Information whether changes occurred
:rtype: dict
```
#### manual_voldrv_arakoon_checkup
```
Creates a new Arakoon Cluster if required and extends cluster if possible on all available master nodes
:return: True if task completed, None if task was discarded (by decorator)
:rtype: bool|None
```
#### mark_offline
```
Marks all StorageDrivers on this StorageRouter offline
:param storagerouter_guid: Guid of the Storage Router
:type storagerouter_guid: str
:return: None
```
#### refresh_configuration
```
Refresh the StorageDriver's configuration (Configuration must have been updated manually)
:param storagedriver_guid: Guid of the StorageDriver
:type storagedriver_guid: str
:return: Amount of changes the volumedriver detected
:rtype: int
```
#### scheduled_voldrv_arakoon_checkup
```
Makes sure the volumedriver arakoon is on all available master nodes
:return: None
```
#### volumedriver_error
```
Handles error messages/events from the volumedriver
:param code: Volumedriver error code
:type code: int
:param volume_id: Name of the volume throwing the error
:type volume_id: str
:return: None
```
### Storagerouter
#### add_vpool
```
Add a vPool to the machine this task is running on
:param parameters: Parameters for vPool creation
:type parameters: dict
:return: None
:rtype: NoneType
```
#### configure_disk
```
Configures a partition
:param storagerouter_guid: Guid of the StorageRouter to configure a disk on
:type storagerouter_guid: str
:param disk_guid: Guid of the disk to configure
:type disk_guid: str
:param partition_guid: Guid of the partition on the disk
:type partition_guid: str
:param offset: Offset for the partition
:type offset: int
:param size: Size of the partition
:type size: int
:param roles: Roles assigned to the partition
:type roles: list
:return: None
:rtype: NoneType
```
#### configure_support
```
Configures support on all StorageRouters
:param support_info: Information about which components should be configured
    {'stats_monkey': True,  # Enable/disable the stats monkey scheduled task
     'support_agent': True,  # Responsible for enabling the ovs-support-agent service, which collects heart beat data
     'remote_access': False,  # Cannot be True when support agent is False. Is responsible for opening an OpenVPN tunnel to allow for remote access
     'stats_monkey_config': {}}  # Dict with information on how to configure the stats monkey (Only required when enabling the stats monkey
:type support_info: dict
:return: None
:rtype: NoneType
```
#### create_hprm_config_files
```
Create the required configuration files to be able to make use of HPRM (aka PRACC)
This configuration will be zipped and made available for download
:param vpool_guid: The guid of the VPool for which a HPRM manager needs to be deployed
:type vpool_guid: str
:param local_storagerouter_guid: The guid of the StorageRouter the API was requested on
:type local_storagerouter_guid: str
:param parameters: Additional information required for the HPRM configuration files
:type parameters: dict
:return: Name of the zipfile containing the configuration files
:rtype: str
```
#### get_logfiles
```
Collects logs, moves them to a web-accessible location and returns log tgz's filename
:param local_storagerouter_guid: StorageRouter guid to retrieve log files on
:type local_storagerouter_guid: str
:return: Name of tgz containing the logs
:rtype: str
```
#### get_metadata
```
Gets physical information about the specified StorageRouter
:param storagerouter_guid: StorageRouter guid to retrieve the metadata for
:type storagerouter_guid: str
:return: Metadata information about the StorageRouter
:rtype: dict
```
#### get_proxy_config
```
Gets the ALBA proxy for a given StorageRouter and vPool
:param storagerouter_guid: Guid of the StorageRouter on which the ALBA proxy is configured
:type storagerouter_guid: str
:param vpool_guid: Guid of the vPool for which the proxy is configured
:type vpool_guid: str
:return: The ALBA proxy configuration
:rtype: dict
```
#### get_support_info
```
Returns support information for the entire cluster
:return: Support information
:rtype: dict
```
#### get_support_metadata
```
Returns support metadata for a given StorageRouter. This should be a routed task!
:return: Metadata of the StorageRouter
:rtype: dict
```
#### get_version_info
```
Returns version information regarding a given StorageRouter
:param storagerouter_guid: StorageRouter guid to get version information for
:type storagerouter_guid: str
:return: Version information
:rtype: dict
```
#### mountpoint_exists
```
Checks whether a given mount point for a vPool exists
:param name: Name of the mount point to check
:type name: str
:param storagerouter_guid: Guid of the StorageRouter on which to check for mount point existence
:type storagerouter_guid: str
:return: True if mount point not in use else False
:rtype: bool
```
#### ping
```
Update a StorageRouter's celery heartbeat
:param storagerouter_guid: Guid of the StorageRouter to update
:type storagerouter_guid: str
:param timestamp: Timestamp to compare to
:type timestamp: float
:return: None
:rtype: NoneType

Editor input:
Called by a cronjob which is placed under/etc/cron.d/openvstorage-core on install of the openvstorage - core package
```
#### refresh_hardware
```
Refreshes all hardware related information
:param storagerouter_guid: Guid of the StorageRouter to refresh the hardware on
:type storagerouter_guid: str
:return: None
:rtype: NoneType
```
### Update
#### get_update_metadata
```
Returns metadata required for updating
  - Checks if 'at' is installed properly
  - Checks if ongoing updates are busy
  - Check if StorageRouter is reachable
:param storagerouter_ip: IP of the StorageRouter to check the metadata for
:type storagerouter_ip: str
:return: Update status for specified StorageRouter
:rtype: dict
```
#### merge_downtime_information
```
This is called upon by the Update overview page when clicking the 'Update' button to show the prerequisites which have not been met and downtime issues
Merge the downtime information and prerequisite information of all StorageRouters and plugins (ALBA, iSCSI, ...) per component
This contains information about
    - downtime of model, GUI, vPools, proxies, Arakoon clusters, ...
    - prerequisites that have not been met
:return: Information about the update
:rtype: dict
```
#### merge_package_information
```
This is called upon by the Update overview page, to show all updates grouped by IP, being either a StorageRouter, ALBA Node, iSCSI Node, ...
Merge the package information of all StorageRouters and plugins (ALBA, iSCSI, ...) per IP
:return: Package information for all StorageRouters and ALBA nodes
:rtype: dict
```
#### update_components
```
Initiate the update through commandline for all StorageRouters
This is called upon by the API
:return: None
```
### Vdisk
#### clone
```
Clone a vDisk
:param vdisk_guid: Guid of the vDisk to clone
:type vdisk_guid: str
:param name: Name of the new clone (can be a path or a user friendly name)
:type name: str
:param snapshot_id: ID of the snapshot to clone from
:type snapshot_id: str
:param storagerouter_guid: Guid of the StorageRouter
:type storagerouter_guid: str
:param pagecache_ratio: Ratio of the page cache size (compared to a 100% cache)
:type pagecache_ratio: float
:param cache_quota: Max disk space the new clone can consume for caching (both fragment as block) purposes (in Bytes)
:type cache_quota: dict
:return: Information about the cloned volume
:rtype: dict
```
#### create_from_template
```
Create a vDisk from a template
:param vdisk_guid: Guid of the vDisk
:type vdisk_guid: str
:param name: Name of the newly created vDisk (can be a filename or a user friendly name)
:type name: str
:param storagerouter_guid: Guid of the Storage Router on which the vDisk should be started
:type storagerouter_guid: str
:param pagecache_ratio: Ratio of the page cache size (compared to a 100% cache)
:type pagecache_ratio: float
:param cache_quota: Max disk space the new volume can consume for caching purposes (in Bytes)
:type cache_quota: dict
:return: Information about the new volume (vdisk_guid, name, backingdevice)
:rtype: dict
```
#### create_new
```
Create a new vDisk/volume using hypervisor calls
:param volume_name: Name of the vDisk (can be a filename or a user friendly name)
:type volume_name: str
:param volume_size: Size of the vDisk
:type volume_size: int
:param storagedriver_guid: Guid of the Storagedriver
:type storagedriver_guid: str
:param pagecache_ratio: Ratio of the page cache size (compared to a 100% cache)
:type pagecache_ratio: float
:param cache_quota: Max disk space the new volume can consume for caching purposes (in Bytes)
:type cache_quota: dict
:return: Guid of the new vDisk
:rtype: str
```
#### create_snapshot
```
Create a vDisk snapshot
:param vdisk_guid: Guid of the vDisk
:type vdisk_guid: str
:param metadata: Dictionary of metadata
:type metadata: dict
:return: ID of the newly created snapshot
:rtype: str
```
#### create_snapshots
```
Create vDisk snapshots
:param vdisk_guids: Guid of the vDisks
:type vdisk_guids: list
:param metadata: Dictionary of metadata
:type metadata: dict
:return: ID of the newly created snapshot
:rtype: dict
```
#### delete
```
Delete a vDisk through API
:param vdisk_guid: Guid of the vDisk to delete
:type vdisk_guid: str
:return: None
```
#### delete_from_voldrv
```
Delete a vDisk from model only since its been deleted on volumedriver
Triggered by volumedriver messages on the queue
:param volume_id: Volume ID of the vDisk
:type volume_id: str
:return: None
```
#### delete_snapshot
```
Delete a vDisk snapshot
:param vdisk_guid: Guid of the vDisk
:type vdisk_guid: str
:param snapshot_id: ID of the snapshot
:type snapshot_id: str
:return: None
```
#### delete_snapshots
```
Delete vDisk snapshots
:param snapshot_mapping: Mapping of VDisk guid and Snapshot ID(s)
:type snapshot_mapping: dict
:return: Information about the deleted snapshots, whether they succeeded or not
:rtype: dict
```
#### dtl_checkup
```
Check DTL for all volumes, for all volumes of a vPool or for 1 specific volume
DTL allocation rules:
    - First priority to StorageRouters located in the vDisk's StorageRouter's Recovery Domain
    - Second priority to StorageRouters located in the vDisk's StorageRouter's Regular Domain
    - If Domains configured, but no StorageRouters are found matching any of the Domains on the vDisk's StorageRouter, a random SR in the same vPool is chosen
    - If no Domains configured on the vDisk StorageRouter, any other StorageRouter on which the vPool has been extended is chosen

:param vpool_guid: vPool to check the DTL configuration of all its vDisks
:type vpool_guid: str
:param vdisk_guid: vDisk to check its DTL configuration
:type vdisk_guid: str
:param storagerouters_to_exclude: Storage Router Guids to exclude from possible targets
:type storagerouters_to_exclude: list
:return: None
:rtype: NoneType
```
#### dtl_state_transition
```
Triggered by volumedriver when DTL state changes
:param volume_id: ID of the volume
:type volume_id: str
:param old_state: Previous DTL status
:type old_state: int
:param new_state: New DTL status
:type new_state: int
:param storagedriver_id: ID of the storagedriver hosting the volume
:type storagedriver_id: str
:return: None
```
#### extend
```
Extend a vDisk through API
:param vdisk_guid: Guid of the vDisk to extend
:type vdisk_guid: str
:param volume_size: New size in bytes
:type volume_size: int
:return: None
```
#### get_config_params
```
Retrieve the configuration parameters for the given vDisk from the storagedriver.
:param vdisk_guid: Guid of the vDisk to retrieve the configuration for
:type vdisk_guid: str
:return: Storage driver configuration information for the vDisk
:rtype: dict
```
#### is_volume_synced_up_to_snapshot
```
Verify if a volume is synced up to a specific snapshot
:param vdisk_guid: Guid of vDisk to verify
:type vdisk_guid: str
:param snapshot_id: Snapshot_id to verify
:type snapshot_id: str
:return: True or False
:rtype: bool
```
#### is_volume_synced_up_to_tlog
```
Verify if a volume is synced up to a specific tlog
:param vdisk_guid: Guid of vDisk to verify
:type vdisk_guid: str
:param tlog_name: Tlog_name to verify
:type tlog_name: str
:return: True or False
:rtype: bool
```
#### list_volumes
```
List all known volumes on a specific vpool or on all
:param vpool_guid: Guid of the vPool to list the volumes for
:type vpool_guid: str
:return: Volumes known by the vPool or all volumes if no vpool_guid is provided
:rtype: list
```
#### migrate_from_voldrv
```
Triggered when volume has changed owner (Clean migration or stolen due to other reason)
Triggered by volumedriver messages
:param volume_id: Volume ID of the vDisk
:type volume_id: unicode
:param new_owner_id: ID of the storage driver the volume migrated to
:type new_owner_id: unicode
:return: None
```
#### move
```
Move a vDisk to the specified StorageRouter
:param vdisk_guid: Guid of the vDisk to move
:type vdisk_guid: str
:param target_storagerouter_guid: Guid of the StorageRouter to move the vDisk to
:type target_storagerouter_guid: str
:param force: Indicates whether to force the migration or not (forcing can lead to data loss)
:type force: bool
:return: None
```
#### move_multiple
```
Move list of vDisks to the specified StorageRouter
:param vdisk_guids: Guids of the vDisk to move
:type vdisk_guids: list
:param target_storagerouter_guid: Guid of the StorageRouter to move the vDisk to
:type target_storagerouter_guid: str
:param force: Indicates whether to force the migration or not (forcing can lead to data loss)
:type force: bool
:return: None
```
#### rename_from_voldrv
```
Processes a rename event from the volumedriver. At this point we only expect folder renames. These folders
might contain vDisks. Although the vDisk's .raw file cannot be moved/renamed, the folders can.
:param old_path: The old path (prefix) that is renamed
:type old_path: str
:param new_path: The new path (prefix) of that folder
:type new_path: str
:param storagedriver_id: The StorageDriver's ID that executed the rename
:type storagedriver_id: str
:return: None
:rtype: NoneType
```
#### resize_from_voldrv
```
Resize a vDisk
Triggered by volumedriver messages on the queue
:param volume_id: volume ID of the vDisk
:type volume_id: str
:param volume_size: Size of the volume
:type volume_size: int
:param volume_path: Path on hypervisor to the volume
:type volume_path: str
:param storagedriver_id: ID of the storagedriver serving the volume to resize
:type storagedriver_id: str
:return: None
```
#### restart
```
Restart the given vDisk
:param vdisk_guid: The guid of the vDisk to restart
:type vdisk_guid: str
:param force: Force a restart at a possible cost of data loss
:type force: bool
:return: None
:rtype: NoneType
```
#### rollback
```
Rolls back a vDisk based on a given vDisk snapshot timestamp
:param vdisk_guid: Guid of the vDisk to rollback
:type vdisk_guid: str
:param timestamp: Timestamp of the snapshot to rollback from
:type timestamp: str
:return: True
:rtype: bool
```
#### schedule_backend_sync
```
Schedule a backend sync on a vDisk
:param vdisk_guid: Guid of vDisk to schedule a backend sync to
:type vdisk_guid: str
:return: TLogName associated with the data sent off to the backend
:rtype: str
```
#### set_as_template
```
Set a vDisk as template
:param vdisk_guid: Guid of the vDisk
:type vdisk_guid: str
:return: None
```
#### set_config_params
```
Sets configuration parameters for a given vDisk.
DTL allocation rules:
    - First priority to StorageRouters located in the vDisk's StorageRouter's Recovery Domain
    - Second priority to StorageRouters located in the vDisk's StorageRouter's Regular Domain
    - If Domains configured, but no StorageRouters are found matching any of the Domains on the vDisk's StorageRouter, a random SR in the same vPool is chosen
    - If no Domains configured on the vDisk StorageRouter, any other StorageRouter on which the vPool has been extended is chosen

:param vdisk_guid: Guid of the vDisk to set the configuration parameters for
:type vdisk_guid: str
:param new_config_params: New configuration parameters
:type new_config_params: dict
:return: None
```
#### sync_with_reality
```
Syncs vDisks in the model with reality
:param vpool_guid: Optional vPool guid. All vPools if omitted
:type vpool_guid: str or None
:return: None
:rtype: NoneType
```
### Vpool
#### balance_change
```
Execute a balance change. Balances can be calculated through ovs.lib.helpers.vdisk.rebalancer.VDiskRebalancer
This task is created to offload the balance change to Celery to get concurrency across VPools
:param vpool_guid: Guid of the VPool to execute the balance changes for. Used for ensure_single and validation
:type vpool_guid: str
:param execute_only_for_srs: Guids of StorageRouters to perform the balance change for (if not specified, executed for all)
:type execute_only_for_srs: Optional[List[str]]
:param exported_balances: List of exported balances
:type exported_balances: List[dict]
:return:
```
#### shrink_vpool
```
Removes a StorageDriver (if its the last StorageDriver for a vPool, the vPool is removed as well)
:param storagedriver_guid: Guid of the StorageDriver to remove
:type storagedriver_guid: str
:param offline_storage_router_guids: Guids of StorageRouters which are offline and will be removed from cluster.
                                     WHETHER VPOOL WILL BE DELETED DEPENDS ON THIS
:type offline_storage_router_guids: list
:return: None
:rtype: NoneType
```
#### up_and_running
```
Volumedriver informs us that the service is completely started. Post-start events can be executed
:param storagedriver_id: ID of the storagedriver
```
