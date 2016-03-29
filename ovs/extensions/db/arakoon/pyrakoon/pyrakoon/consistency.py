# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2014 Incubaid BVBA
#
# Licensees holding a valid Incubaid license may use this file in
# accordance with Incubaid's Arakoon commercial license agreement. For
# more information on how to enter into this agreement, please contact
# Incubaid (contact details can be found on www.arakoon.org/licensing).
#
# Alternatively, this file may be redistributed and/or modified under
# the terms of the GNU Affero General Public License version 3, as
# published by the Free Software Foundation. Under this license, this
# file is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# See the GNU Affero General Public License for more details.
# You should have received a copy of the
# GNU Affero General Public License along with this program (file "COPYING").
# If not, see <http://www.gnu.org/licenses/>.

'''Client request result consistency definitions'''

import operator

class Consistency(object): #pylint: disable=R0903
    '''Abstract base class for consistency definition types'''


class Consistent(Consistency): #pylint: disable=R0903
    '''Representation of the 'Consistent' consistency policy'''

    def __repr__(self):
        return 'CONSISTENT'

CONSISTENT = Consistent()
'''The `CONSISTENT` consistency policy''' #pylint: disable=W0105
del Consistent


class Inconsistent(Consistency): #pylint: disable=R0903
    '''Representation of the 'Inconsistent' consistency policy'''

    def __repr__(self):
        return 'INCONSISTENT'

INCONSISTENT = Inconsistent()
'''The `INCONSISTENT` consistency policy''' #pylint: disable=W0105
del Inconsistent


class AtLeast(Consistency): #pylint: disable=R0903
    '''Representation of an 'at least' consistency policy'''

    __slots__ = '_i',

    def __init__(self, i):
        '''Create an 'at least' consistency policy definition

        :param i: Minimal required `i` value
        :type i: `int`
        '''

        self._i = i #pylint: disable=C0103

    def __repr__(self):
        return 'AtLeast(%d)' % self.i

    i = property(operator.attrgetter('_i'), doc='Minimal \'i\'')

