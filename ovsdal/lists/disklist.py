from ovsdal.dataobject import DataObject


class DiskList(object):
    @staticmethod
    def disks_from_machine(machine):
        guid = machine.guid if DataObject.is_dataobject(machine) else machine
        return None