from ovs.lib.vmachine import VMachine
from ovs.dal.lists.vmachinelist import VMachineList

import subprocess


class Ovs():

    my_machine_id = ''
    my_vsa_guid = ''
    my_vsr_id = ''

    @staticmethod
    def execute_command(cmd, catch_output=True):
        """
        Executes a command
        """
        if catch_output:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = process.stdout.readlines()
            process.wait()
            return process.returncode, output
        else:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            output = process.communicate()
            return process.returncode, output

    @staticmethod
    def get_my_machine_id():
        """
        Returns unique machine id based on mac address
        """
        if not Ovs.my_machine_id:
            cmd = """ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g' | sort"""
            rcode, output = Ovs.execute_command(cmd, catch_output=False)
            for id in output[0].strip().split('\n'):
                if id.strip() != '000000000000':
                    Ovs.my_machine_id = id.strip()
                    break
        return Ovs.my_machine_id

    @staticmethod
    def get_my_vsa():
        """
        Returns unique machine vsa id
        """
        if not Ovs.my_vsa_guid:
            for vm in VMachineList().get_vmachines():
                if vm.is_internal and vm.machineid == Ovs.get_my_machine_id():
                    Ovs.my_vsa_guid = vm.guid
        return VMachine(Ovs.my_vsa_guid)

    @staticmethod
    def get_my_vsr_id(vpool_name):
        """
        Returns unique machine vsrid based on vpool_name and machineid
        """
        return vpool_name + Ovs.get_my_machine_id()
