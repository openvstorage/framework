#!/usr/bin/python2
#  Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Performance unittest module
"""
import time
import sys
from unittest import TestCase
from ovs.dal.hybrids.t_testdisk import TestDisk
from ovs.dal.hybrids.t_testmachine import TestMachine
from ovs.dal.datalist import DataList
from ovs.dal.exceptions import ObjectNotFoundException


class LotsOfObjects(TestCase):
    """
    Executes a performance test by working with a large set of objects
    """

    def test_lotsofobjects(self):
        """
        A test creating, linking and querying a lot of objects
        """
        print ''
        print 'cleaning up'
        self._clean_all()
        print 'start test'
        if getattr(LotsOfObjects, 'amount_of_machines', None) is None:
            LotsOfObjects.amount_of_machines = 50
        if getattr(LotsOfObjects, 'amount_of_disks', None) is None:
            LotsOfObjects.amount_of_disks = 5
        load_data = True
        if load_data:
            print 'start loading data'
            start = time.time()
            mguids = []
            min_run, max_run = 999999, 0
            for i in xrange(0, int(LotsOfObjects.amount_of_machines)):
                mstart = time.time()
                machine = TestMachine()
                machine.name = 'machine_{0}'.format(i)
                machine.save()
                mguids.append(machine.guid)
                for ii in xrange(0, int(LotsOfObjects.amount_of_disks)):
                    disk = TestDisk()
                    disk.name = 'disk_{0}_{1}'.format(i, ii)
                    disk.size = ii * 100
                    disk.machine = machine
                    disk.save()
                avgitemspersec = ((i + 1) * LotsOfObjects.amount_of_disks) / (time.time() - start)
                itemspersec = LotsOfObjects.amount_of_disks / (time.time() - mstart)
                min_run = min(min_run, itemspersec)
                max_run = max(max_run, itemspersec)
                self._print_progress('* machine {0}/{1} (run: {2} dps, avg: {3} dps)'.format(i + 1, int(LotsOfObjects.amount_of_machines), round(itemspersec, 2), round(avgitemspersec, 2)))
            print '\nloading done. min: {0} dps, max: {1} dps'.format(round(min_run, 2), round(max_run, 2))

        test_queries = True
        if test_queries:
            print 'start queries'
            start = time.time()
            min_run, max_run = 999999, 0
            for i in xrange(0, int(LotsOfObjects.amount_of_machines)):
                mstart = time.time()
                machine = TestMachine(mguids[i])
                self.assertEqual(len(machine.disks), LotsOfObjects.amount_of_disks, 'Not all disks were retreived ({0})'.format(len(machine.disks)))
                avgitemspersec = ((i + 1) * LotsOfObjects.amount_of_disks) / (time.time() - start)
                itemspersec = LotsOfObjects.amount_of_disks / (time.time() - mstart)
                min_run = min(min_run, itemspersec)
                max_run = max(max_run, itemspersec)
                self._print_progress('* machine {0}/{1} (run: {2} dps, avg: {3} dps)'.format(i + 1, int(LotsOfObjects.amount_of_machines), round(itemspersec, 2), round(avgitemspersec, 2)))
            print '\ncompleted. min: {0} dps, max: {1} dps'.format(round(min_run, 2), round(max_run, 2))

            print 'start full query on disk property'
            start = time.time()
            amount = DataList({'object': TestDisk,
                               'data': DataList.select.COUNT,
                               'query': {'type': DataList.where_operator.AND,
                                         'items': [('size', DataList.operator.GT, 100),
                                                   ('size', DataList.operator.LT, (LotsOfObjects.amount_of_disks - 1) * 100)]}}).data
            self.assertEqual(amount, (LotsOfObjects.amount_of_disks - 3) * LotsOfObjects.amount_of_machines, 'Incorrect amount of disks. Found {0} instead of {1}'.format(amount, int((LotsOfObjects.amount_of_disks - 3) * LotsOfObjects.amount_of_machines)))
            seconds_passed = (time.time() - start)
            print 'completed in {0} seconds (avg: {1} dps)'.format(round(seconds_passed, 2), round(LotsOfObjects.amount_of_machines * LotsOfObjects.amount_of_disks / seconds_passed, 2))

        clean_data = True
        if clean_data:
            print 'cleaning up'
            start = time.time()
            min_run, max_run = 999999, 0
            for i in xrange(0, int(LotsOfObjects.amount_of_machines)):
                mstart = time.time()
                machine = TestMachine(mguids[i])
                for disk in machine.disks:
                    disk.delete()
                machine.delete()
                avgitemspersec = ((i + 1) * LotsOfObjects.amount_of_disks) / (time.time() - start)
                itemspersec = LotsOfObjects.amount_of_disks / (time.time() - mstart)
                min_run = min(min_run, itemspersec)
                max_run = max(max_run, itemspersec)
                self._print_progress('* machine {0}/{1} (run: {2} dps, avg: {3} dps)'.format(i + 1, int(LotsOfObjects.amount_of_machines), round(itemspersec, 2), round(avgitemspersec, 2)))
            print '\ncompleted. min: {0} dps, max: {1} dps'.format(round(min_run, 2), round(max_run, 2))

    def _print_progress(self, message):
        """
        Prints progress (overwriting)
        """
        sys.stdout.write('\r{0}'.format(message))
        sys.stdout.flush()

    def _clean_all(self):
        """
        Cleans all disks and machines
        """
        machine = TestMachine()
        keys = DataList.get_pks(machine._namespace, machine._name)
        for guid in keys:
            try:
                machine = TestMachine(guid)
                for disk in machine.disks:
                    disk.delete()
                machine.delete()
            except (ObjectNotFoundException, ValueError):
                DataList.delete_pk(machine._namespace, machine._name, guid)
        disk = TestDisk()
        keys = DataList.get_pks(disk._namespace, disk._name)
        for guid in keys:
            try:
                disk = TestDisk(guid)
                disk.delete()
            except (ObjectNotFoundException, ValueError):
                DataList.delete_pk(disk._namespace, disk._name, guid)

if __name__ == '__main__':
    import unittest
    LotsOfObjects.amount_of_machines = float(sys.argv[1])
    LotsOfObjects.amount_of_disks = float(sys.argv[2])
    suite = unittest.TestLoader().loadTestsFromTestCase(LotsOfObjects)
    unittest.TextTestRunner(verbosity=2).run(suite)
