# This file is part of Pyrakoon, a distributed key-value store client.
#
# Copyright (C) 2013, 2014 Incubaid BVBA
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

'''Administrative client interface'''

from pyrakoon.client import utils
from pyrakoon.protocol import admin

class ClientMixin: #pylint: disable=W0232,C1001
    '''Mixin providing client actions for node administration

    This can be mixed into any class implementing
    :class:`pyrakoon.client.AbstractClient`.
    '''

    #pylint: disable=C0111,R0201
    @utils.call(admin.OptimizeDB) #pylint: disable=E1101
    def optimize_db(self):
        assert False

    @utils.call(admin.DefragDB) #pylint: disable=E1101
    def defrag_db(self):
        assert False

    @utils.call(admin.DropMaster) #pylint: disable=E1101
    def drop_master(self):
        assert False

    @utils.call(admin.CollapseTlogs) #pylint: disable=E1101
    def collapse_tlogs(self):
        assert False

    @utils.call(admin.FlushStore) #pylint: disable=E1101
    def flush_store(self):
        assert False
