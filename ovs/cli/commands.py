# Copyright (C) 2019 iNuron NV
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


from ovs_extensions.cli import OVSCommand as _OVSCommand


class UnittestCommand(_OVSCommand):

    """
    Command used to run the unittests with
    """

    def invoke(self, ctx):
        """
        Invoke the command
        """
        # from ovs.extensions.log import configure_logging
        # configure_logging()

        super(UnittestCommand, self).invoke(ctx)


class OVSCommand(_OVSCommand):

    """
    Command used to run ovs commands with
    """

    def invoke(self, ctx):
        """
        Invoke the command
        """
        from ovs.extensions.log import configure_logging
        configure_logging()

        super(OVSCommand, self).invoke(ctx)
