# Docker environments (draft spec not ready to go)

## goals

- allow user to install an openvstorage environment for demo/test in < 1h
- promote OVS to docker users
- use docker environment to test/develop OVS

## what

- use all build dockers which are other story card
- create a very easy to use python script which puts the configuration items in ETCD per component (make modular)

## required dockers

for each docker make sure there is a docker repo
and in docker hub we build it automatically (see story card about building dockers)

all dockers based on ubuntu 15.10 SSH docker (this allows remote management for users if they want to)
will be easy for users to change this behaviour

in all docker repo's do not enable issues/wiki (that needs to be driven from parent repo)

### etcd
- etcd for config mgmt
- use arguments to set main login/passwd to etcd

### 1 or more arakoon
- only argument = link to etcd with login/passwd
- docker volume for data

### 1 or more alba
- only argument = link to etcd with login/passwd
- 1 or more docker volumes for data

### volumedriver for iscsi
- only argument = link to etcd with login/passwd

### volumedriver for fuse
- only argument = link to etcd with login/passwd
- docker will have to be started in privileged mode (otherwise fuse cannot expose the filesystem, not 100% sure this works)
  - if not possible then we will find other very easy way to configure host to use OVS fuse
  
### volumedriver for qemu
- only argument = link to etcd with login/passwd
- I think if docker started in priviliged mode it is possible to allow qemu on host to use vol driver in docker (lets investigate)
  - if not possible then we will find other very easy way to configure host to use OVS qemu

### OVS controller
- full framework installed in 1 docker !
- use SSH to get into the dockers & deploy the agent code (use rsync over ssh), evaluate the cuisine functionality in jumpscale8

## remarks
- installer OVS framework is NOT being used !!! debian/rpm installers is not being used
