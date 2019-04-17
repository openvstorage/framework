### Snapshot management
The Framework will, by default, create snapshots of every vDisk every hour 
(can be adjusted. See docs/scheduledtasks.md).

To keep the snapshots manageable overtime, the Framework schedules a clean-up every day to enforce a retention policy.
This automatic task will:
- Create an overview of the all the snapshots for every volume
- Skip the first 24 hours (allows the user to create as many snaphots as he wants daily)
- Enforce the retention policy

The default retention policy is:
- a single snapshot is kept for the first 7 days after that
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
Since we are working with days, *monthly and weekly policies will not follow the calendar days!*

There are two additional options available: `consistency_first` 
which indicates that:
- this policy has to search for the oldest consistent snapshot instead of oldest one
- When no consistent snapshot was found, find the oldest snapshot

If a policy interval spans multiple days, the `consistency_first_on` can be configured to narrow the days down 
to apply the `consistency_first` rules
This options takes in a list of day numbers.


If we were to write out the default retention policy, it would look like:
```
[# one per day for the week and opt for a consistent snapshot for the first day
 {'nr_of_snapshots': 7, 'nr_of_days': 7, 'consistency_first': True, 'consistency_first_on': [1]},
 # One per week for the rest of the month
 {'nr_of_snapshots': 3, 'nr_of_days': 21}]
```

Configuring it on different levels can be done using the API:
- Global level: POST to: `'/storagerouters/<storagerouter_guid>/global_snapshot_retention_policy'`
- vPool level: POST to: `/vpools/<vpool_guid>/snapshot_retention_policy`
- vDisk level: POST to: `/vdisks/<vdisk_guid>/snapshot_retention_policy`

##### Examples:
The examples simplify a week as 7 days and months as 4 * 7 days.

I wish to keep hourly snapshots from the first week
```
[{'nr_of_days': 7,  # A week spans 7 days
  'nr_of_snapshots': 168}]  # Keep 24 snapshot for every day for 7 days: 7 * 24
```
I wish to keep hourly snapshots from the first week and one for every week for the whole year
```
[ # First policy
  {'nr_of_days': 7,  # A week spans 7 days
  'nr_of_snapshots': 7 * 24},  # Keep 24 snapshot for every day for 7 days: 7 * 24
  # Second policy
  {'nr_of_days': 7 * (52 - 1),  # The first week is already covered by the previous policy, so 52 - 1 weeks remaining
   'nr_of_snapshots': 1 * (52 - 1)}
]
```

A production use case could be:
```
[ # First policy - keep the first 24 snapshots
  {'nr_of_days': 1,
  'nr_of_snapshots': 24 },
  # Second policy - Keep 4 snapshots a day for the remaining week (6 leftover days)
  {'nr_of_days': 6,
   'nr_of_snapshots': 4 * 6},
  # Third policy - keep 1 snapshot per day for the 3 weeks to come
  {'nr_of_days': 3 * 7,
   'nr_of_snapshots': 3 * 7]
  # Fourth policy - keep 1 snapshot per week for the next 5 months
  {'nr_of_days': 4 * 7 * 5,  # Use the week notation to avoid issues (4 * 7 days = month)
   'nr_of_snapshots': 5 * 7
  # Fift policy - first 6 months are configured by now - Keep a snapshot every 6 month until 2 years have passed
   {'nr_of_days': (4 * 7) * (6 * 3),
    'nr_of_snapshots': 3}
 ] 
   ```