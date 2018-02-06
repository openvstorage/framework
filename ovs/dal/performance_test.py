#!/usr/bin/env python2
# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Performance unittest module
"""
import sys
import time
import uuid
import random

from ovs.dal.hybrids.t_testdisk import TestDisk
from ovs.dal.hybrids.t_testmachine import TestMachine
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObject
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.helpers import DalToolbox
from ovs.extensions.storage.persistentfactory import PersistentFactory


# noinspection PyProtectedMember
class LotsOfObjects(object):
    """
    Executes a performance test by working with a large set of objects
    """
    def __init__(self):
        """
        Init method
        """
        self.persistent = PersistentFactory.get_client()

    def test_lotsofobjects(self):
        """
        A test creating, linking and querying a lot of objects
        """
        print ''
        print 'cleaning up'
        self._clean_all()

        print 'preparing'
        if getattr(LotsOfObjects, 'amount_of_machines', None) is None:
            LotsOfObjects.amount_of_machines = 50
        else:
            LotsOfObjects.amount_of_machines = int(LotsOfObjects.amount_of_machines)
        if getattr(LotsOfObjects, 'amount_of_disks', None) is None:
            LotsOfObjects.amount_of_disks = 5
        else:
            LotsOfObjects.amount_of_disks = int(LotsOfObjects.amount_of_disks)
        if getattr(LotsOfObjects, 'repetition_scan', None) is None:
            LotsOfObjects.repetition_scan = 32
        else:
            LotsOfObjects.repetition_scan = int(LotsOfObjects.repetition_scan)
        total_amount_of_disks = LotsOfObjects.amount_of_machines * LotsOfObjects.amount_of_disks
        mguids = []
        uuids = [str(uuid.uuid4()) for _ in xrange(total_amount_of_disks)]
        counter = 0
        repetition = []
        for i in xrange(LotsOfObjects.repetition_scan):
            repetition.append([])
        dguids = []

        print 'start test'
        tstart = time.time()

        print '\nstart loading data'
        start = time.time()
        runtimes = []
        for i in xrange(LotsOfObjects.amount_of_machines):
            mstart = time.time()
            machine = TestMachine()
            machine.name = 'machine_{0}'.format(i)
            machine.save()
            mguids.append(machine.guid)
            for ii in xrange(LotsOfObjects.amount_of_disks):
                current_uuid = uuids[counter]
                disk = TestDisk()
                disk.name = 'disk_{0}_{1}'.format(i, ii)
                disk.description = 'disk_{0}'.format(i)
                disk.size = ii * 100
                disk.machine = machine
                disk.something = current_uuid
                disk.save()
                dguids.append(disk.guid)
                random.choice(repetition).append(current_uuid)
                counter += 1
            avgitemspersec = ((i + 1) * LotsOfObjects.amount_of_disks) / (time.time() - start)
            itemspersec = LotsOfObjects.amount_of_disks / (time.time() - mstart)
            runtimes.append(itemspersec)
            LotsOfObjects._print_progress('* machine {0}/{1} (run: {2:.2f} dps, avg: {3:.2f} dps)'.format(i + 1, LotsOfObjects.amount_of_machines, itemspersec, avgitemspersec))
        runtimes.sort()
        print '\nloading done ({0:.2f}s). min: {1:.2f} dps, max: {2:.2f} dps'.format(time.time() - tstart, runtimes[1], runtimes[-2])

        test_queries = True
        if test_queries:
            print '\nstart queries'
            start = time.time()
            runtimes = []
            for i in xrange(LotsOfObjects.amount_of_machines):
                mstart = time.time()
                machine = TestMachine(mguids[i])
                assert len(machine.disks) == LotsOfObjects.amount_of_disks, 'Not all disks were retrieved ({0})'.format(len(machine.disks))
                avgitemspersec = ((i + 1) * LotsOfObjects.amount_of_disks) / (time.time() - start)
                itemspersec = LotsOfObjects.amount_of_disks / (time.time() - mstart)
                runtimes.append(itemspersec)
                LotsOfObjects._print_progress('* machine {0}/{1} (run: {2:.2f} dps, avg: {3:.2f} dps)'.format(i + 1, LotsOfObjects.amount_of_machines, itemspersec, avgitemspersec))
            runtimes.sort()
            print '\ncompleted ({0:.2f}s). min: {1:.2f} dps, max: {2:.2f} dps'.format(time.time() - tstart, runtimes[1], runtimes[-2])

            print '\nstart full query on disk property'
            start = time.time()
            dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': [('size', DataList.operator.GT, 100),
                                                  ('size', DataList.operator.LT, (LotsOfObjects.amount_of_disks - 1) * 100)]})
            amount = len(dlist)
            assert amount == (max(0, LotsOfObjects.amount_of_disks - 3)) * LotsOfObjects.amount_of_machines, 'Incorrect amount of disks. Found {0} instead of {1}'.format(amount, int((max(0, LotsOfObjects.amount_of_disks - 3)) * LotsOfObjects.amount_of_machines))
            seconds_passed = time.time() - start
            print 'completed ({0:.2f}s) in {1:.2f} seconds (avg: {2:.2f} dps)'.format(time.time() - tstart, seconds_passed, total_amount_of_disks / seconds_passed)

            print '\nloading disks (all)'
            start = time.time()
            for i in xrange(LotsOfObjects.amount_of_machines):
                machine = TestMachine(mguids[i])
                _ = [_ for _ in machine.disks]
            seconds_passed = time.time() - start
            print 'completed ({0:.2f}s) in {1:.2f} seconds (avg: {2:.2f} dps)'.format(time.time() - tstart, seconds_passed, total_amount_of_disks / seconds_passed)

            print '\nstart full query on disk property (using cached objects)'
            dlist._volatile.delete(dlist._key)
            start = time.time()
            dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': [('size', DataList.operator.GT, 100),
                                                  ('size', DataList.operator.LT, (LotsOfObjects.amount_of_disks - 1) * 100)]})
            amount = len(dlist)
            assert amount == (max(0, LotsOfObjects.amount_of_disks - 3)) * LotsOfObjects.amount_of_machines, 'Incorrect amount of disks. Found {0} instead of {1}'.format(amount, int((max(0, LotsOfObjects.amount_of_disks - 3)) * LotsOfObjects.amount_of_machines))
            seconds_passed = time.time() - start
            print 'completed ({0:.2f}s) in {1:.2f} seconds (avg: {2:.2f} dps)'.format(time.time() - tstart, seconds_passed, total_amount_of_disks / seconds_passed)

            print '\nindexed single query'
            dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': [('something', DataList.operator.EQUALS, repetition[0][0])]})
            start = time.time()
            assert len(dlist) == 1, 'One disk should be found'
            seconds_passed = time.time() - start
            print 'completed ({0:.2f}s) in {1:.3f} seconds (avg: {2:.2f} dps)'.format(time.time() - tstart, seconds_passed, total_amount_of_disks / seconds_passed)

            print '\nstart property sort'
            dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': []})
            start = time.time()
            dlist.sort(key=lambda a: DalToolbox.extract_key(a, 'size'))
            seconds_passed = time.time() - start
            print 'completed ({0:.2f}s) in {1:.2f} seconds (avg: {2:.2f} dps)'.format(time.time() - tstart, seconds_passed, total_amount_of_disks / seconds_passed)

            print '\nstart dynamic sort'
            dlist._volatile.delete(dlist._key)
            dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': []})
            start = time.time()
            dlist.sort(key=lambda a: DalToolbox.extract_key(a, 'predictable'))
            seconds_passed = time.time() - start
            print 'completed ({0:.2f}s) in {1:.2f} seconds (avg: {2:.2f} dps)'.format(time.time() - tstart, seconds_passed, total_amount_of_disks / seconds_passed)

            print '\nrepetition scan'
            start = time.time()
            times = []
            for i in xrange(LotsOfObjects.repetition_scan):
                dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                            'items': [('something', DataList.operator.IN, repetition[i])]})
                run_start = time.time()
                assert len(repetition[i]) == len(dlist), 'Incorrect amount of found disks. Found {0} instead of {1}'.format(len(dlist), len(repetition[i]))
                times.append(time.time() - run_start)
            seconds_passed = time.time() - start
            print 'completed ({0:.2f}s) in {1:.2f} seconds (run avg: {2:.3f}s, avg: {3:.2f} dps)'.format(time.time() - tstart, seconds_passed, sum(times) / float(LotsOfObjects.repetition_scan), LotsOfObjects.repetition_scan * total_amount_of_disks / seconds_passed)

            print '\nguid index query'
            start = time.time()
            guids = dguids[len(dguids)/2:]
            dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': [('guid', DataList.operator.IN, guids)]})
            assert len(dlist) == len(guids), 'Incorrect amount of found disks. Found {0} instead of {1}'.format(len(dlist), len(guids))
            seconds_passed = time.time() - start
            print 'completed ({0:.2f}s) in {1:.2f} seconds (avg: {2:.2f} dps)'.format(time.time() - tstart, seconds_passed, total_amount_of_disks / seconds_passed)

        clean_data = True
        if clean_data:
            print '\ncleaning up'
            start = time.time()
            runtimes = []
            for i in xrange(0, int(LotsOfObjects.amount_of_machines)):
                mstart = time.time()
                machine = TestMachine(mguids[i])
                for disk in machine.disks:
                    disk.delete()
                machine.delete()
                avgitemspersec = ((i + 1) * LotsOfObjects.amount_of_disks) / (time.time() - start)
                itemspersec = LotsOfObjects.amount_of_disks / (time.time() - mstart)
                runtimes.append(itemspersec)
                LotsOfObjects._print_progress('* machine {0}/{1} (run: {2:.2f} dps, avg: {3:.2f} dps)'.format(i + 1, LotsOfObjects.amount_of_machines, itemspersec, avgitemspersec))
            runtimes.sort()
            print '\ncompleted ({0:.2f}s). min: {1:.2f} dps, max: {2:.2f} dps'.format(time.time() - tstart, runtimes[1], runtimes[-2])

    @staticmethod
    def _print_progress(message):
        """
        Prints progress (overwriting)
        """
        sys.stdout.write('\r{0}    '.format(message))
        sys.stdout.flush()

    def _clean_all(self):
        """
        Cleans all disks and machines
        """
        machine = TestMachine()
        prefix = '{0}_{1}_'.format(DataObject.NAMESPACE, machine._classname)
        keys = self.persistent.prefix(prefix)
        for key in keys:
            try:
                guid = key.replace(prefix, '')
                machine = TestMachine(guid)
                for disk in machine.disks:
                    disk.delete()
                machine.delete()
            except (ObjectNotFoundException, ValueError):
                pass
        for prefix in ['ovs_reverseindex_{0}', 'ovs_unique_{0}', 'ovs_index_{0}']:
            for key in self.persistent.prefix(prefix.format(machine._classname)):
                self.persistent.delete(key)
        disk = TestDisk()
        prefix = '{0}_{1}_'.format(DataObject.NAMESPACE, disk._classname)
        keys = self.persistent.prefix(prefix)
        for key in keys:
            try:
                guid = key.replace(prefix, '')
                disk = TestDisk(guid)
                disk.delete()
            except (ObjectNotFoundException, ValueError):
                pass
        for prefix in ['ovs_reverseindex_{0}', 'ovs_unique_{0}', 'ovs_index_{0}']:
            for key in self.persistent.prefix(prefix.format(disk._classname)):
                self.persistent.delete(key)

if __name__ == '__main__':
    if len(sys.argv) >= 3:
        LotsOfObjects.amount_of_machines = float(sys.argv[1])
        LotsOfObjects.amount_of_disks = float(sys.argv[2])
    if len(sys.argv) >= 4:
        LotsOfObjects.repetition_scan = float(sys.argv[3])
    performance = LotsOfObjects()
    performance.test_lotsofobjects()
