### Framework Packager
#### Jenkins environments

| http://10.100.129.100:8080/view/Packaging/job/qa-packaging/  |  repo  |
|---|---|
| 
  | openvstorage-test  | integration tests
    openvstorage-health-check  | health-check  |
|  openvstorage-automation-lib | automation lib, needed for the integration tests and nightly installs  |
| openvstorage-setup-runner  | setup-runner, for nightly installs  |

| http://10.100.129.100:8080/view/framework/job/framework-packaging/  |  repo  |
|---|---|
| 
  | openvstorage  | fwk
    openvstorage-backend  | alba-plugin  |
    openvstorage-sdm  | asd-manager  |
    openvstorage-extensions  | ovs-extensions  |
    openvstorage-iscsi-plugin  | ovs-iscsi-plugin  |
    iscsi-manager  | iscsi-manager  |
    hprm-manager  | hprm-manager (deprecated) |
    openvstorage-s3-manager  | s3-manager (deprecated)  |




| http://10.100.129.100:8080/view/Packaging/job/operations_packaging/  |  repo  |
|---|---|
| 
  | dev-ops  | ??
    openvstorage-support-tools | support-tools  |


#### How to use
1. top left list: `build with parameters`
2. choose product
3. choose release:
     - `develop`: builds develop branch of chosen repo. No need to specify `revision` 
     - `experimental`: make an experimental package of chose repo. Needs a specified `revision` for the wanted branch one wants to make a package for. Does not build a release
     - `master`: builds master branch of chosen repo. No need to specify `revision`
     - `hotfix`: builds a specified branch, and make a release for it. Makes a new release for this version. This option is used for packaging current andes 3 release (fwk 2.9.x)
     
4. Artifact only: only builds [artifacts](https://jenkins.io/doc/pipeline/tour/tests-and-artifacts/)
5. no_upload: make a release and packages, but don't upload to our repo
6. dry_run: debugging tool. Don't make release or tags, only test if packaging works. Prints all commands.

#### Packager code
The packager code is located in the [framework-tools](https://github.com/openvstorage/framework-tools)

| file  |  usage  |
|---|---|
| 
  | `jenkins_commands.sh`  | point of entry, calls `packager.py`
    `packager.py`  | collects input parameters, calls needed packager, contains main flow  |
    `repo-maintenance`  |   |
    `sourcecollector.py`  | parses parameters passed to jenkins |
    `settings.json`  | contains everything the packager needs such as codepaths, github repo paths etc.  |
    `packagers.debian.py`  | debian packager  |
    `packagers.packager.py`  | base class with parent functionality for all packagers |
    `packagers.pip.py`  | package `pip `packages to `deb`. Useful when a new debendency is created (such as python-typing, boto (s3 stuff)   |
    `packagers.redhat.py`  | package to redhat. (deprecated) |
