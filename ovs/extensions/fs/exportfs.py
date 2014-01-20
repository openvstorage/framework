# license see http://www.openvstorage.com/licenses/opensource/
import re
import subprocess


class Nfsexports(object):
    """
    Basic management for /etc/exports
    """
    def __init__(self):
        self._exportsFile = '/etc/exports'
        self._cmd = ['/usr/bin/sudo', '-u', 'root', '/usr/sbin/exportfs']

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
        dlist = [i.strip() for i in dlist if not i.startswith('#')]
        dlist = [re.split('\s+|\(|\)', i) for i in dlist]
        keys = ['dir', 'network', 'params']
        ldict = [dict(zip(keys, line)) for line in dlist]

        return ldict

    def add(self, directory, network, params):
        """
        Add entry to /etc/exports

        @param directory: directory to export
        @param network: network range allowed
        @param params: params for export (eg, 'ro,async,no_root_squash,no_subtree_check')
        """
        l = self._slurp()
        for i in l:
            if i['dir'] == directory:
                print 'Directory already exported, to export with different params please first remove'
                return
        f = open(self._exportsFile, 'a')
        f.write('%s %s(%s)\n' % (directory, network, params))
        f.close()

    def remove(self, directory):
        """
        Remove entry from /etc/exports
        """
        l = self._slurp()
        for i in l:
            if i['dir'] == directory:
                l.remove(i)
                f = open(self._exportsFile, 'w')
                for i in l:
                    f.write("%s %s(%s) \n" % (i['dir'], i['network'], i['params']))
                f.close()
                return

    def list_exported(self):
        """
        List the current exported filesystems
        """
        exports = {}
        for export in subprocess.check_output(self._cmd).splitlines():
            directory, network = export.split('\t')
            exports[directory.strip()] = network.strip()
        return exports

    def unexport(self, directory):
        """
        Unexport a filesystem
        """
        cmd = list(self._cmd)
        exports = self.list_exported()
        if not directory in exports.keys():
            print 'Directory %s currently not exported' % directory
            return
        print 'Unexporting {}:{}'.format(exports[directory] if exports[directory] != '<world>' else '*', directory)
        cmd.extend(['-u', '{}:{}'.format(exports[directory] if exports[directory] != '<world>' else '*', directory)])
        subprocess.call(cmd)

    def export(self, directory, network='*'):
        """
        Export a filesystem
        """
        cmd = list(self._cmd)
        exports = self.list_exported()
        if directory in exports.keys():
            print 'Directory already exported with options %s' % exports[directory]
            return
        print 'Exporting {}:{}'.format(network, directory)
        cmd.extend(['-v', '{}:{}'.format(network, directory)])
        subprocess.call(cmd)
