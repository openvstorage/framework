# license see http://www.openvstorage.com/licenses/opensource/
import re

class Nfsexports(object):
    """
    Basic management for /etc/exports
    """
    def __init__(self):
        self._exportsFile     = '/etc/exports'

    def _slurp(self):
        """
        Read from /etc/exports
        """
        f = open(self._exportsFile, 'r')
        dlist = []
        for line in f:
            if not re.match('^\s*$', line):
                dlist.append(line)
        f.close()
        dlist = [ i.strip() for i in dlist if not i.startswith('#') ]
        dlist = [ re.split('\s+|\(|\)',i) for i in dlist ]
        keys=['dir','network','params']
        ldict = [ dict(zip(keys,line)) for line in dlist ]

        return ldict

    def add(self, dir, network, params):
        """
        Add entry to /etc/exports

        @param dir: directory to export
        @param network: network range allowed
        @param params: params for export (eg, 'ro,async,no_root_squash,no_subtree_check')
        """
        l = self._slurp()
        for i in l:
            if i['dir'] == dir:
                print 'Directory already exported, to export with different params please first remove'
                return
        f = open(self._exportsFile, 'a')
        f.write('%s %s(%s)\n' % (dir, network, params))
        f.close

    def remove(self, dir):
        """
        Remove entry from /etc/exports
        """
        l = self._slurp()
        for i in l:
            if i['dir'] == dir:
                l.remove(i)
                f = open(self._exportsFile, 'w')
                for i in l:
                    f.write("%s %s(%s) \n" % ( i['dir'], i['network'], i['params']))
                f.close()
                return