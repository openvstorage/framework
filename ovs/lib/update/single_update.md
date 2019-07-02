# Single update
The Framework now supports updating certain components on a single host at the time.
It provides simple CLI commands (`ovs local_update <component>`) to update Arakoon, Alba and the volumedriver

 ## Alba update
The Volumedriver update can be started by running `ovs local_update alba`.
The local update will:
- Update the volumedriver binaries. Currently, only andes-updates-3 towards andes-updates-4 is supported. Further updates towards bighorn will need further testing, as alba will bump from 1.5.x towards 1.6.x.
    
- Update the alba alternatives that were introduced. 
`/usr/bin/alba` will from now on be a symlink towards a binary.
`/opt/alba<version>/bin/alba`. Some plugin information, needed for correct functioning of service files is changed too.
- Update the arakoon alternatives that were introduced. `/usr/bin/arakoon` will now point to
`/opt/alba<version>/bin/arakoon`
Note that given the nature of arakoon and its polling, stacktraces of nomaster exceptions may occure. These errors can be negated. This logging could not be suppressed, as essential other output could be suppressed with it.

 ### Exceptions
Certain exception can be thrown during the update.

 | Exit code | Exception name | What happened |
| --------- | ---------------| ------------- |
| 61        | NoMasterFoundException | Raise this error when no arakoon master can be found after a couple of ComponentUpdaterattempts |
| 62        | InvalidAlbaVersionException | Will be called if no valid alba version has been found and the update-alternatives call has failed with this alba version|