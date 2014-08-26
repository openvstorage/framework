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
Basic test module
"""
import uuid
import sys
import time
from random import shuffle
from unittest import TestCase
from ovs.dal.hybrids.t_testmachine import TestMachine
from ovs.dal.datalist import DataList


class Extended(TestCase):
    """
    The extended unittestsuite will test a few more extended functions of the framework. It can be executed in
    integration tests, and if the tested codepaths change (since it's slower than the basic tests)
    """

    def test_pk_stretching(self):
        """
        Validates whether the primary key lists scale correctly.
        * X entries will be added (e.g. 10000)
        * X/2 random entries will be deleted (5000)
        * X/2 entries will be added again (5000)
        * X entries will be removed (10000)
        No entries should be remaining
        """
        print ''
        print 'starting test'
        amount_of_objects = 10000  # Must be an even number!
        machine = TestMachine()
        runtimes = []
        # First fill
        start = time.time()
        keys = DataList._get_pks(machine._namespace, machine._name)
        self.assertEqual(len(list(keys)), 0, 'There should be no primary keys yet ({0})'.format(len(list(keys))))
        guids = []
        mstart = time.time()
        for i in xrange(0, amount_of_objects):
            guid = str(uuid.uuid4())
            guids.append(guid)
            DataList.add_pk(machine._namespace, machine._name, guid)
            keys = DataList._get_pks(machine._namespace, machine._name)
            self.assertEqual(len(list(keys)), len(guids), 'There should be {0} primary keys instead of {1}'.format(len(guids), len(list(keys))))
            if i % 100 == 99:
                avgitemspersec = (i + 1) / (time.time() - start)
                itemspersec = 100 / (time.time() - mstart)
                runtimes.append(itemspersec)
                self._print_progress('* adding object {0}/{1} (run: {2} ops, avg: {3} ops)'.format(i + 1, int(amount_of_objects), round(itemspersec, 2), round(avgitemspersec, 2)))
                mstart = time.time()
        print ''
        # First delete
        amount_of_objects /= 2
        shuffle(guids)  # Make the test a bit more realistic
        guids_copy = guids[:]
        dstart = time.time()
        mstart = time.time()
        for i in xrange(0, amount_of_objects):
            guid = guids_copy[i]
            guids.remove(guid)
            DataList.delete_pk(machine._namespace, machine._name, guid)
            keys = DataList._get_pks(machine._namespace, machine._name)
            self.assertEqual(len(list(keys)), len(guids), 'There should be {0} primary keys instead of {1}'.format(len(guids), len(list(keys))))
            if i % 100 == 99:
                avgitemspersec = (i + 1) / (time.time() - dstart)
                itemspersec = 100 / (time.time() - mstart)
                runtimes.append(itemspersec)
                self._print_progress('* delete object {0}/{1} (run: {2} ops, avg: {3} ops)'.format(i + 1, int(amount_of_objects), round(itemspersec, 2), round(avgitemspersec, 2)))
                mstart = time.time()
        keys = DataList._get_pks(machine._namespace, machine._name)
        self.assertEqual(len(list(keys)), amount_of_objects, 'There should be {0} primary keys ({1})'.format(amount_of_objects, len(list(keys))))
        print ''
        # Second round
        sstart = time.time()
        mstart = time.time()
        for i in xrange(0, amount_of_objects):
            guid = str(uuid.uuid4())
            guids.append(guid)
            DataList.add_pk(machine._namespace, machine._name, guid)
            keys = DataList._get_pks(machine._namespace, machine._name)
            self.assertEqual(len(list(keys)), len(guids), 'There should be {0} primary keys instead of {1}'.format(len(guids), len(list(keys))))
            if i % 100 == 99:
                avgitemspersec = (i + 1) / (time.time() - sstart)
                itemspersec = 100 / (time.time() - mstart)
                runtimes.append(itemspersec)
                self._print_progress('* adding object {0}/{1} (run: {2} ops, avg: {3} ops)'.format(i + 1, int(amount_of_objects), round(itemspersec, 2), round(avgitemspersec, 2)))
                mstart = time.time()
        print ''
        # Second delete
        amount_of_objects *= 2
        shuffle(guids)  # Make the test a bit more realistic
        guids_copy = guids[:]
        dstart = time.time()
        mstart = time.time()
        for i in xrange(0, amount_of_objects):
            guid = guids_copy[i]
            guids.remove(guid)
            DataList.delete_pk(machine._namespace, machine._name, guid)
            keys = DataList._get_pks(machine._namespace, machine._name)
            self.assertEqual(len(list(keys)), len(guids), 'There should be {0} primary keys instead of {1}'.format(len(guids), len(list(keys))))
            if i % 100 == 99:
                avgitemspersec = (i + 1) / (time.time() - dstart)
                itemspersec = 100 / (time.time() - mstart)
                runtimes.append(itemspersec)
                self._print_progress('* delete object {0}/{1} (run: {2} ops, avg: {3} ops)'.format(i + 1, int(amount_of_objects), round(itemspersec, 2), round(avgitemspersec, 2)))
                mstart = time.time()
        keys = DataList._get_pks(machine._namespace, machine._name)
        self.assertEqual(len(guids), 0, 'All guids should be removed. {0} left'.format(len(guids)))
        self.assertEqual(len(list(keys)), 0, 'There should be no primary keys ({0})'.format(len(list(keys))))
        seconds_passed = (time.time() - start)
        runtimes.sort()
        print '\ncompleted in {0} seconds (avg: {1} ops, min: {2} ops, max: {3} ops)'.format(round(seconds_passed, 2), round((amount_of_objects * 3) / seconds_passed, 2), round(runtimes[1], 2), round(runtimes[-2], 2))

    def _print_progress(self, message):
        """
        Prints progress (overwriting)
        """
        sys.stdout.write('\r{0}    '.format(message))
        sys.stdout.flush()

if __name__ == '__main__':
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Extended)
    unittest.TextTestRunner(verbosity=2).run(suite)
