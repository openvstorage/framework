# Remove hypervisor dependencies

## Scope
We want to make Open vStorage a real storage product which no longer depends on a hypervisor. In previous versions we always assumed that the Volume Driver ran together with a hypervisor (VMware/KVM). With the Edge this no longer the case as the edge client runs on the compute nodes (hypervisor) and the volume driver runs separately on a different node.
With the feature request we want to break this hypervisor dependency. This means you can now install Open vStorage and create a vdisk and add the hypervisor later.

## OVS setup
* Remove the question(s) and code about the hypervisor (VMware and ESXi)

## Hypervisors
### NFS
* Remove the NFS path for VMware (VMware will be supported through iSCSI)
### KVM
* For KVM we need to still support the FUSE filesystem approach
* When creating a vDisk on the fuse layer we don't interact with th hypervisor to find out which VM it is attached too.
* .raw only
### Edge
* The Edge should be able to work without the FUSe layer
* The Edge should have an optional FUSE view on the disks (.raw)

## GUI
* vPool detail: remove #vMachines
 * Add/extend vPool: remove auto-configuration screen
* vMachines: remove ity completely
* vDisks
 * Overview: remove vMachine column add Edge column
 * Detail: update breadcrumb to remove vMachine, remove vMachine
* Remove vMachine Templates 
* Administration remove hypervisor management

## API & DAL
* remove vmachine, pmachine and hypervisor management center

## OpenStack
* The Open vStorage Cinder driver should work with the Edge client
* We will no longer automatically configure the Nova nodes
  * Remove the automatic configuration
  * Provide documentation steps on how to configure/remove Nova with Open vStorage Cinder

## Open question 
* What info can we display of the edge(s)?
 * Client info?
 * Server info
 * For iSCSI disks we should be able to select a HA edge

