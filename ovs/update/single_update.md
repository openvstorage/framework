# Single update
The Framework now supports updating certain components on a single host at the time.
It provides simple CLI commands (`ovs local_update <component>`) to update Arakoon, Alba and the volumedriver

## Volumedriver update
The Volumedriver update can be started by running `ovs local_update volumedriver`.
The local update will:
- Update the volumedriver binaries
- Update the node distance map to avoid HA-ing to the node being upgraded
- Move away all volumes from the node to other nodes
- Migrate away all MDS master instances running on the node
- Restart the volumedriver services
- Update the node distance map to accept HA back onto the node

### Exceptions
Certain exception can be thrown during the update.

| Exit code | Exception name | What happened |
| --------- | ---------------| ------------- |
| 21        | FailedToMigrateException | Not all volumes can be migrated away to different nodes. No moves have started yet |
| 22        | FailureDuringMigrateException | Some volumes could not be moved away during the migration process |
