## Unit Testing

### Usage
We have 2 ways of using the unit tests
* CLI
* Shell

##### CLI
* Listing tests
```ovs unittest list```
```
/opt/OpenvStorage/ovs/dal/tests/test_alba
/opt/OpenvStorage/ovs/dal/tests/test_basic
/opt/OpenvStorage/ovs/dal/tests/test_hybrids
/opt/OpenvStorage/ovs/extensions/generic/tests/test_system
/opt/OpenvStorage/ovs/extensions/hypervisor/tests/test_interfaces
/opt/OpenvStorage/ovs/lib/tests/mdsservice_tests/test_mdsservice
/opt/OpenvStorage/ovs/lib/tests/scheduledtask_tests/test_deletesnapshots
/opt/OpenvStorage/ovs/lib/tests/vdisk_tests/test_dtl_checkup
/opt/OpenvStorage/ovs/lib/tests/vdisk_tests/test_vdisk
/opt/OpenvStorage/webapps/api/tests/test_authentication
/opt/OpenvStorage/webapps/api/tests/test_decorators
```
* Running tests
```ovs unittest``` --> Run all tests
```ovs unittest /opt/OpenvStorage/ovs/dal``` --> Run DAL tests
```ovs unittest /opt/OpenvStorage/ovs/dal,/opt/OpenvStorage/ovs/lib``` --> Run DAL tests and LIB tests
```ovs unittest /opt/OpenvStorage/ovs/dal/tests/test_hybrids``` --> Run DAL hybrid tests
```ovs unittest /opt/OpenvStorage/ovs/dal/tests/test_hybrids.Hybrid``` --> Run tests in the DAL Hybrid Class
```ovs unittest /opt/OpenvStorage/ovs/dal/tests/test_hybrids.Hybrid:test_objectproperties``` --> Run specific test in the DAL Hybrid Class
* Example output
```
root@OVS-1:~\# ovs unittest /opt/OpenvStorage/ovs/extensions

\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#
\# Processing test-module /opt/OpenvStorage/ovs/extensions/generic/tests/test_system \#
\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#

test_check_if_single_port_is_free (test_system.TestSystem) ... ok
test_first_free_port_after_system_range_succeeds (test_system.TestSystem) ... ok
test_get_1_free_port (test_system.TestSystem) ... ok
test_get_2_free_ports (test_system.TestSystem) ... ok
test_local_remote_check (test_system.TestSystem) ... ok
test_no_free_port_can_be_found_within_system_range (test_system.TestSystem) ... ok
test_support_for_multiple_port_ranges (test_system.TestSystem) ... ok

----------------------------------------------------------------------
Ran 7 tests in 0.940s

OK

\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#
\# Processing test-module /opt/OpenvStorage/ovs/extensions/hypervisor/tests/test_interfaces \#
\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#\#

test_hypervisors (test_interfaces.Interfaces) ...

                            | VMware | KVM | Equal parameters
----------------------------+--------+-----+-----------------
test_connection             | x      | x   | x
get_rename_scenario         | x      | x   | x
mount_nfs_datastore         | x      | x   | x
get_vmachine_path           | x      | x   | x
get_state                   | x      | x   | x
create_volume               | x      | x   | x
get_vms_by_nfs_mountinfo    | x      | x   | x
delete_vm                   | x      | x   | x
get_vm_object               | x      | x   | x
__init__                    | x      | x   | x
clone_vm                    | x      | x   | x
create_vm_from_template     | x      | x   | x
get_vm_object_by_devicename | x      | x   | x
extend_volume               | x      | x   | x
get_vm_agnostic_object      | x      | x   | x
is_datastore_available      | x      | x   | x
set_as_template             | x      | x   | x
delete_volume               | x      | x   | x
clean_backing_disk_filename | x      | x   | x
get_disk_path               | x      | x   | x
get_backing_disk_path       | x      | x   | x
clean_vmachine_filename     | x      | x   | x
file_exists                 | x      | x   | x
should_process              | x      | x   | x

ok
test_mgmtcenters (test_interfaces.Interfaces) ...

                              | OpenStack | VCenter | Equal parameters
------------------------------+-----------+---------+-----------------
test_connection               | x         | x       | x
is_host_configured            | x         | x       | x
get_vdisk_model_by_devicepath | x         | x       | x
get_vm_agnostic_object        | x         | x       | x
get_guests                    | x         | x       | x
configure_host                | x         | x       | x
is_host_configured_for_vpool  | x         | x       | x
get_host_status_by_pk         | x         | x       | x
unconfigure_vpool_for_host    | x         | x       | x
unconfigure_host              | x         | x       | x
configure_vpool_for_host      | x         | x       | x
get_guest_by_guid             | x         | x       | x
__init__                      | x         | x       | x
get_vdisk_device_info         | x         | x       | x
get_hosts                     | x         | x       | x
get_host_status_by_ip         | x         | x       | x
get_vmachine_device_info      | x         | x       | x
get_host_primary_key          | x         | x       | x

ok

----------------------------------------------------------------------
Ran 2 tests in 0.056s

OK



\#\#\#\#\#\#\#\#\#\#\#\#\#
\# OVERVIEW \#
\#\#\#\#\#\#\#\#\#\#\#\#\#

  - TestModule: /opt/OpenvStorage/ovs/extensions/generic/tests/test_system  (7 tests)
    - DURATION: < 1 second
    - SUCCESS: 7

  - TestModule: /opt/OpenvStorage/ovs/extensions/hypervisor/tests/test_interfaces  (2 tests)
    - DURATION: < 1 second
    - SUCCESS: 2


\#\#\#\#\#\#\#\#\#\#\#\#
\# SUMMARY \#
\#\#\#\#\#\#\#\#\#\#\#\#

  - Total amount of tests: 9
  - Total duration: < 1 second
    - SUCCESS: 9 / 9 (100.00 %)
```

### Flaws
The way the Framework handles unittesting is a bit flawed.
The environment variable is set by the the code which lists the tests. This way we weave in `mocked` instances of certain implementations.

However due to the nature of loading in modules and thus compiling them at runtime, we could end up with real implementation as opposed to mocked ones.

We try to work around these issues by 
- Substituting clients (like in Configuration with its _passthrough)
- Offloading to factory patterns (like packagefactory/servicefactory)

