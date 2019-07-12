# Update
The update of the Framework is managed by the UpdateController. The Framework provides a way to update the complete
cluster in one go. Mixing up versions is not supported as there is no versioned coping system and 
workers of a different version could do things differently than the newer version.

## Plugins
Plugins are updated through the main UpdateController by calling the respective hooks. These hooked functions
return data about the packages to update.

This could actually be offloaded to the package manager itself. DPKG offers triggers to be executed upon updating a certain package.
The plugins could setup these trigger-listeners for their own and do an update when the package was updated.
## Drawbacks
The cluster has to be updated in one go. This means that the API and Workers are down during the complete update
- Unavailability
- No mixing support
- Unscalable
- Slow in bigger environments
  - All update data is fetched upfront for all nodes in a cluster
- Hard to recover