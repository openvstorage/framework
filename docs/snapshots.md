### Snapshot management
The Framework will, by default, create snapshots of every vDisk every hour 
(can be adjusted. See docs/scheduledtasks.md).

To keep the snapshots manageable overtime, the Framework schedules a clean-up every day to enforce a retention policy.
This automatic task will:
- Create an overview of the all the snapshots for every volume
- Skip the first 24 hours (allows the user to create as many snaphots as he wants daily)
- Enforce the retention policy

The default retention policy is:
- an hourly snapshot is kept for yesterday
- a single snapshot is kept for the 6 days after that
    - Prioritizes consistent snapshots over older ones for the first day in the policy
     (which is 2 days back, starting from now)
- A single snapshot is kept for the 2nd, 3rd and 4th week to have a single snapshot of the week for the first month
- All older snapshots are discarded

#### Configuring the retention policy
A retention policy can be configured so the scheduled task will enforce a different one from the default.

It can be customized on:
- Global level, enforces the policy to all vDisks within the cluster
- VPool level, overrides the global level, enforce to all vDisks within the vPool
- VDisk level, overrides the global and vPool level, enforce to this vDisk only

The notation of the policy is a list containing policies. A policies consists minimally of `nr_of_snapshots`, which
is the the number of snapshots  to have over the given `nr_of_days`, and `nr_of_days` which is the number of days to span
the `nr_of_snapshots` over. This notation allows for some fine grained control while also being easy to configure.

There are two additional options available: `consistency_first` 
which indicates that:
- this policy has to search for the oldest consistent snapshot instead of oldest one
- When no consistent snapshot was found, find the oldest snapshot

If a policy interval spans multiple days, the `consistency_first_on` can be configured to narrow the days down 
to apply the `consistency_first` rules
This options takes in a list of day numbers.

If we were to write out the default retention policy, it would look like:
```
[# One per hour
 {'nr_of_snapshots': 24, 'nr_of_days': 1},  
 # one per day for rest of the week and opt for a consistent snapshot for the first day
 {'nr_of_snapshots': 6, 'nr_of_days': 6, 'consistency_first': True, 'consistency_first_on': [1]},
 # One per week for the rest of the month
 {'nr_of_snapshots': 3, 'nr_of_days': 21}]
```

Configuring it on different levels can be done using the API:
- Global level: <>
- vPool level: <>
- vDisk level: <>

Example:
```
Insert
```