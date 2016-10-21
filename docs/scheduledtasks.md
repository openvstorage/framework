### The scrubber
The scrubber process is responsible for freeing up backend storage space by removing data which is no longer of any use.  The very definition of data being no longer of use is SCOs (the actual data) and tlogs (the metadata) belonging to snapshots that where deleted in which LBAs are referenced that are over written in one of the next snapshot.
This might sound a little complication but the example will clear things up.

For our (simplified) example assume the following:

* A volume has 3 snapshots at present.
* Block size is 4KB
* Snapshot number 2's tlogs reference writes to blocks on the volume with LBAs 1000 and 2000 through 2500.
* Snapshot number 3's tlogs reference writes to blocks on the volume with LBAs 2000 through 2400.
* We delete snapshot number 2

LBA 2000 to 2400 are changed in the snapshot (Snapshot 3) right after the one we deleted. This means those transactions done in snapshot 2 no longer need to be kept.
Changes to LBAs 1000 and 2401 to 2500 need to be applied to snapshot3 as we need those to be able to replay tlogs to get the original data.

The effective disk space gained by this operation (without counting overhead) is 400 blocks with each block being 4KB in size, 1.5625 MB of freed space.

The actual scrubbing process is a 3 step process:
1. Get scrub work: List the snapshots which have the flag scrubbed to false and from these snapshots  get all the tlogs and referenced SCOs that need to be scrubbed.
2. Scrubbing: The previous list is being worked through from the oldest tlog to newest tlog. Only one volume and tlog is scrubbed at a time. New SCOs and tlogs are written, the current ones are NOT changed and not yet deleted. At the end only the last reference is kept for each LBA in the tlogs and the actual data is stored in a SCO.
3. Apply scrubbed data: The scrubbed new SCOs and tlogs are put to the backend,  metadata is modified and if there are no issues, the old tlogs and SCOs are deleted.

Note that the scrub process only runs on Storage Routers/nodes with the Scrubbing role and only the result of the scrubwork is applied on the node where the volume is running. This has as benefit that hosts running the actual Virtual Machines don't have to waste resources to the intensive scrub process.

Let's now look into more details:

A typical snapshot xml of a VM will contain a list of snapshots and within these snapshots the tlogs are listed (reference between the LBA and the data stored in a SCO) which make up the actual snapshot.

```
...
<snapshot>
    <tlogs>
        <tlog1>
        <tlog2>
    </tlogs>
</snapshot>
<snapshot>
    <tlogs>
        <tlog3>
        <tlog4>
    </tlogs>
</snapshot>
<snapshot>
    <tlogs>
        <tlog5>
        <tlog6>
    </tlogs>
</snapshot>
...
```

When snapshot 2 is removed the snapshot xml will be updated
```
...
<snapshot>
    <tlogs>
        <tlog1>
        <tlog2>
    </tlogs>
</snapshot>
<snapshot>
    <tlogs>
        <tlog3>
        <tlog4>
        <tlog5>
        <tlog6>
    </tlogs>
</snapshot>
...
```
The last snapshot of the above list will also be received the flag `scrubbed=false`. This will bring the tlogs and SCOs in scope for the next run of the scrubber.

Once the scrubber has run the snapshots xml will look like:
```
...
<snapshot>
    <tlogs>
        <tlog1>
        <tlog2>
    </tlogs>
</snapshot>
<snapshot>
    <tlogs>
        <tlog25>
        <tlog26>
    </tlogs>
</snapshot>
...
```

These new tlogs will reference new SCOs and the old tlogs and SCOs will be removed from the backend.
