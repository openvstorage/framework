from ovsdal.dataobject import DataObject
from ovsdal.hybrids.machine import Machine


class DiskList(object):
    @staticmethod
    def near_full_disks(machine_or_guid, percent=0.9):
        machine = DataObject.fetch_object(Machine, machine_or_guid)
        for disk in machine.disks:
            if disk.size > 0 and (disk.used_size / (disk.size * 1.0)) > percent:
                yield disk