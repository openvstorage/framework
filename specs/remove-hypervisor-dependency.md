# Remove hypervisor dependencies

## Scope
We want to make Open vStorage a real storage product which no longer depends on a hypervisor. In previous versions we always assumed that the Volume Driver ran together with a hypervisor (VMware/KVM). With the Edge this no longer the case as the edge client runs on the compute nodes (hypervisor) and the volume driver runs separately on a different node.
With th FR we want to break this hypervisor dependency. This means you can now install Open vStorage and add the hypervisor later.

## OVS setup
* Remove the question(s) and code about the hypervisor (VMware and ESXi)


## OpenStack
* The Open vStorage Cinder driver should work with the Edge client
* We will no longer automatically configure the Nova nodes
  * Remove the automatic configuration
  * Provide documentation steps on how to configure Nova with Open vStorage Cinder
