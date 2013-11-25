# license see http://www.openvstorage.com/licenses/opensource/
#!/usr/bin/env python
"""
Packaging script
"""

import re
import os
import shutil
import subprocess
from debian.changelog import Changelog, Version
from datetime import datetime
from time import time

class OvsPackaging(object):
    """
    A class to facilitate the following:
      - retrieving code from a repository
        - alternatively updating an existing repository
        - ability to select specific branches
      - based on the code's layout
        - build a distributeable binary package or set of packages
      - upload the binary packages to a server
    Current possibilities:
      - Bitbucket
      - Mercurial
      - Deb files
      - Apt server
    """

    def __init__(self, args):

        if args.repopath:
            self.repopath = os.path.abspath(args.repopath)
        else:
            self.repopath = os.path.join(os.sep, 'opt', 'mercurial', 'openvstorage')

        if args.repository:
            self.repository = args.repository
        else:
            self.repository = 'openvstorage/openvstorage'

        if args.distribution:
            self.distribution = args.distribution
        else:
            self.distribution = 'revision'

        if args.tag:
            self.tag = args.tag
        else:
            self.tag = None

        if args.branch:
            self.branch = args.branch
        else:
            self.branch = 'default'

        if args.promote:
            self.promote = args.promote
        else:
            self.promote = None

        if args.author:
            self.author = args.author
        else:
            self.author = 'OVS Automatic Packager <packager@openvstorage.com>'

        if args.ssh_credentials:
            self.ssh_credentials = args.ssh_credentials
        else:
            self.ssh_credentials = '~/.ssh/id_rsa'

        if args.bb_credentials:
            self.bb_credentials = args.bb_credentials
        else:
            self.bb_credentials = '~/.ssh/id_rsa'

        if args.debug:
            self.debug = args.debug
        else:
            self.debug = False

        self._hg_command = ['hg', '-yq', '--cwd', self.repopath] 
        self._parent_repopath = os.path.split(self.repopath)[0]
        self._needed_directories = ['', 'sources', 'binary']
        self.changelogpath = os.path.join(self.repopath, 'debian', 'changelog')

        if os.path.exists(os.path.expanduser(self.bb_credentials)):
            self._bb_username = None
            self._bb_password = None
        else:
            bb_split = self.bb_credentials.split(':')
            self._bb_username = bb_split[0]
            self._bb_password = ''.join(bb_split[1:])

        if os.path.exists(os.path.expanduser(self.ssh_credentials)):
            self._ssh_username = None
            self._ssh_password = None
        else:
            ssh_split = self.ssh_credentials.split(':')
            self._ssh_username = ssh_split[0]
            self._ssh_password = ''.join(ssh_split[1:])

        self.scripttime = int(time())
        _packagetime = datetime.now()
        if _packagetime.utcoffset():
            self.packagetimestring = _packagetime.strftime('%a, %d %b %Y %H:%M:%S %z')
        else:
            self.packagetimestring = '{} +0000'.format(_packagetime.strftime('%a, %d %b %Y %H:%M:%S'))

        self.incrementversion = False
        self.changelog_action = None
        if (self.tag and self.distribution == 'release'):
            self.incrementversion = True
            self.changelog_action = 'commit'
            if self.promote:
                self.incrementversion = False
        if self.distribution == 'development':
            # perhaps keeping all the recent changes in the changelog 
            # is good here
            # changelog_action = 'revert'
            self.incrementversion = True

    @staticmethod
    def _call(*popenargs, **kwargs):
        retcode = subprocess.call(*popenargs, **kwargs)
        if retcode == 0:
            return
        raise RuntimeError(retcode)

    @staticmethod
    def _check_output(*popenargs, **kwargs):
        output = subprocess.check_output(*popenargs, **kwargs)
        return output

    def process_command(self, commandargs, cwd=None):
        if not isinstance(commandargs, (list, tuple)):
            raise RuntimeError, 'args passed must be in a list'
        print 'Executing {} with working directory {}'.format(commandargs, cwd)
        self._call(commandargs, cwd=cwd)

    @staticmethod
    def _increment_version(versionnumber, increment):
        """
        Increments a <major>.<minor>.<patch>-<build> version number as desired
        This should never increment the build number
        """
        versionlist = versionnumber.split('.')
        major = versionlist[0]
        minor = versionlist[1]
        patchandbuild = versionlist[2]
        patch = patchandbuild.split('-')[0]
        newversion = versionnumber

        if increment == 'major':
            newversion = "{}.{}.{}".format(int(major) + 1, minor, patch)
        elif increment == 'minor':
            newversion = "{}.{}.{}".format(major, int(minor) + 1, patch)
        elif increment == 'patch':
            newversion = "{}.{}.{}".format(major, minor, int(patch) + 1)

        try:
            build = patchandbuild.split('-')[1]
            newversion = "{}-{}".format(newversion, build)
        except IndexError:
            newversion = newversion

        return newversion

    def _gather_version(self):
        """
        extracts version from repository changelog
        returns incremented part of version if indicated or just version
        """

        changelog = Changelog()
        changelogfile = open(self.changelogpath, 'r')

        changelog.parse_changelog(changelogfile)
        currentversion = changelog.full_version.split('~')[0]
        if self.incrementversion:
            version = self._increment_version(currentversion, 'patch')
        else:
            version = currentversion

        changelogfile.close() 

        return version

    def dpkg_source(self, b_or_x, dscpath, options=None, output=None, cwd=None):
        """
        call dpkg-source [options] -b|x dsc
        """

        assert b_or_x in ['-b', '-x']
        dpkgsourceargs = ['/usr/bin/dpkg-source', b_or_x, dscpath]
        if options:
            dpkgsourceargs.insert(1, options)
        if output:
            dpkgsourceargs.append(output)
        self.process_command(dpkgsourceargs, cwd=cwd)

    def _bitbucket_command(self, command):
        """
        check credentials and run specified command
        """
        
        _bb_command = self._hg_command[:]
        _bb_command.extend(command)

        if 'clone' in command:
            _repopath_index = _bb_command.index(self.repopath)
            _bb_command[_repopath_index] = self._parent_repopath

        if self._bb_username and self._bb_password:
            urlstring = 'https://{}:{}@bitbucket.org/{}'
            url = urlstring.format(self._bb_username, 
                    self._bb_password, self.repository)
            _bb_command.append(url)
        else:
            sshstring = 'ssh -i {} -o StrictHostKeyChecking=no '
            sshstring += '-o UserKnownHostsFile=/dev/null' 
            sshcommand = sshstring.format(self.bb_credentials)
            sshurl = 'ssh://hg@bitbucket.org/{}'.format(self.repository)
            _bb_command.extend(['-e', sshcommand, sshurl])

        self.process_command(_bb_command)

    def clone_bitbucket(self):
        """
        Clone the defined mercurial repository from bitbucket
        """

        _clonecommand = ['clone', '-u', self.branch]
        self._bitbucket_command(_clonecommand)

    def pull_bitbucket(self):
        """
        Pull and update the defined mercurial repository from bitbucket
        """

        _pullcommand = ['pull', '-u']
        self._bitbucket_command(_pullcommand)

    def push_bitbucket(self):
        """
        Push the defined mercurial repository to bitbucket
        """

        _pushcommand = ['push']
        self._bitbucket_command(_pushcommand)

    def add_version_to_changelog(self, version):
        """
        add new version to main source changelog
        """

        currentversion = Version(version)
        changelogfile= open(self.changelogpath, 'r')

        changelog = Changelog()
        changelog.parse_changelog(changelogfile)
        if not currentversion in changelog.versions:
            changelog.new_block(
                    package='openvstorage',
                    version=currentversion,
                    distributions='ovs-{}'.format(self.distribution),
                    urgency='low',
                    author=self.author,
                    date=self.packagetimestring)

        change = '\n   * CloudFounders {} Open vStorage release '
        change += '( see changelog for more info on changes )\n'
        changelog.add_change(change.format(version))

        changelogfile = open(self.changelogpath, 'w')
        changelog.write_to_open_file(changelogfile)
        changelogfile.close()

    def act_on_changelog(self, changelog_action, version):
        """
        Act on a changelog in a Bitbucket Mercurial repository
        """

        _push = False
        _hg_command = self._hg_command[:]
        _hg_command.append(changelog_action)
        if changelog_action == 'commit':
            commitmessage = 'ovs release {}'.format(version)
            _hg_command.extend(['-m', commitmessage])
            _push = True
        elif changelog_action == 'revert':
            _hg_command.append('-a')

        _hg_command.append(self.changelogpath)
        self.process_command(_hg_command, cwd=self.repopath)
        if _push:
            self.push_bitbucket()

    def compile_changelog(self, package, version):
        """
        Parse changelog
        Retrieve last revision from current or previous block and request changelog since that revision
        hg log -b default --rev 62: --template '{date|shortdate} {rev} {desc|firstline}\n' ovs
        2013-10-17 60 The classes in ovs.lib are now called <object>Controller
        """
        def _get_last_revision(changelog):
            regex = re.compile('\s+\*\s+[0-9]+-[0-9]+-[0-9]+\s(?P<rev>[0-9]+)\s(?P<desc>.*)')
            for block in changelog._blocks:
                for change in block.changes():
                    m = regex.match(change)
                    if m:
                        return m.groupdict()['rev']
            return '1'

        def _getcommits(sincerevision):
            print 'getting commits since {}'.format(sincerevision)
            commits = list()
            branch = self._check_output(['hg', 'branch']).strip()
            installstring = '{}/debian/{}.install'
            installpath = installstring.format(self.repopath, package)
            installfile = open(installpath, 'r')
            for entry in installfile.readlines():
                sd = entry.split(' ')[0]
                args = ['/usr/bin/hg', 'log', '-b', branch, '--prune', \
                        sincerevision, '--template', \
                        '{date|shortdate} {rev} {desc|firstline}\n', sd]
                commits.extend(self._check_output(args).splitlines())
            commitset = set(commits)
            return sorted(commitset, key=lambda k:str(k).split(' ')[1], reverse=True)

        changelog = Changelog()
        changelogfile = '{}/{}.changelog'.format(self.repopath, package)
        if os.path.exists(changelogfile):
            print 'Changelog file exists'
            changelogfilehandler = open(changelogfile, 'r')
            try:
                changelog.parse_changelog(changelogfilehandler)
            except:
                print 'Changelog parse error, continuing'
        sincerevision = _get_last_revision(changelog)
        commits = _getcommits(sincerevision)
        version_object = Version(version)
        if not version_object in changelog.versions:
            print 'adding new version to changelog {}'.format(version)
            changelog.new_block(package=package,
                                version=version_object,
                                distributions=self.distribution,
                                urgency='low',
                                author=self.author,
                                date=self.packagetimestring)
        if commits:
            for commit in commits:
                changelog.add_change('   * {})'.format(commit))
        else:
            changelog.add_change('   *  No Commits')
        changelogfilehandler = open(changelogfile, 'w')
        changelog.write_to_open_file(changelogfilehandler)
        changelogfilehandler.close()

    def _changelog_version(self):
        """
        """
        versionnumber = self._gather_version()
        if self.distribution == 'development':
            version = '{}~{}~{}'.format(versionnumber,
                    self.scripttime, self.tag)
            self.add_version_to_changelog(version)
            return version
        elif self.tag and self.distribution == 'release':
            version = '{}~{}'.format(versionnumber, self.tag)
            self.add_version_to_changelog(version)
            return version
        else:
            return versionnumber

    def find_src_deb(self, version, sourcepath):
        for sourcedeb in os.listdir(sourcepath):
            print 'found {} looking for file ending with "{}.dsc" in {}'.format(sourcedeb, version, sourcepath)
            if sourcedeb.endswith('{}.dsc'.format(version)):
                sourcedebpath = os.path.join(sourcepath, sourcedeb)

        if not sourcedebpath:
            raise RuntimeError('Source Debian Package Failure - Source Deb Not Found')

        print 'source deb file path: {}'.format(sourcedebpath)
        return sourcedebpath

    def build_dsc(self, version):
        """
        create the source deb
        """

        packages = list()
        cwd = os.path.abspath(os.curdir)
        sourcepath = os.path.join(self._parent_repopath, 'sources')

        os.chdir(self.repopath)
        debianfiles = os.listdir('{}/debian'.format(self.repopath))
        for debfile in debianfiles:
            filename, ext = os.path.splitext(debfile)
            if ext == '.install':
                packages.append(filename)

        for package in packages:
            print 'packaging {} with version {}'.format(package, version)
            self.compile_changelog(package, version)

        exclude_patterns = ['.hg', '.project', '.settings', '.hgignore']
        exclude_args = ''
        for pattern in exclude_patterns:
            exclude_args += '-I{} '.format(pattern)
        os.chdir(sourcepath)
        self.dpkg_source('-b', self.repopath, options=exclude_args)

        os.chdir(cwd)

        return sourcepath

    def dpkg_buildpackage(self, tmpdir, package=None):
        """
        Run buildpackage at a prepared directory
        """
        os.chdir(tmpdir)
        args = ['/usr/bin/dpkg-buildpackage', '-rfakeroot', '-uc', '-us']
        if package:
            args.extend(['-b', '-T', package])
        self.process_command(args)

    def build_deb(self, sourcedebpath, package=None):
        """
        @param sourcedeb: Name of the debian source file(*.dsc)
        """
        if not os.path.exists(sourcedebpath):
            error = 'Source package {} not found'.format(sourcedebpath)
            raise ValueError(error)

        packagebinary = os.path.join(self._parent_repopath, 'binary')
        tmpdir = os.path.join(packagebinary, '{}'.format(self.scripttime))

        os.chdir(packagebinary)
        self.dpkg_source('-x', sourcedebpath, output=tmpdir)

        self.dpkg_buildpackage(tmpdir, package)

    def upload(self, changesfile):
        """
        uploads deb to apt server using dput
        """

        dput_cf_path = os.path.join(self.repopath, 'packaging', 
                'dput.cf')
        dputcommand = ['dput', '-u', '-c', dput_cf_path, 'ovs',
                changesfile]

        if self.debug:
            print 'dput command:\n{}'.format(dputcommand)

        if self._ssh_password:
            import pexpect
            _password = '{}\n'.format(self._ssh_password)
            eventsdict={'(?i)password:': _password}
            pexpect.run(' '.join(dputcommand), events=eventsdict)
        else:
            self.process_command(dputcommand)

    def config_ssh(self):
        """
        Copy configurations from repository to proper places
        """
        config = os.path.expanduser(os.path.join('~', '.ssh'))
        config_path = os.path.join(config, 'config')
        config_temp_name = 'config_{}'.format(self.scripttime)
        config_temp_path = os.path.join(config, config_temp_name)
        config_dput = os.path.join(self.repopath, 'packaging', 
                'dput_ssh_config')

        if os.path.exists(config_path):
            shutil.copyfile(config_path, config_temp_path)

        shutil.copyfile(config_dput, config_path)

        config_file = open(config_path, 'a')
        if self._ssh_username:
            id_config = 'User {}\n'.format(self._ssh_username)
        else:
            id_config = 'IdentityFile {}\n'.format(self.ssh_credentials)
        if not self._bb_username:
            id_config += 'IdentityFile {}\n'.format(self.bb_credentials)
        config_file.write(id_config)
        config_file.close()

    def update_repository(self):
        """
        update repository to specified branch or clone if not around
        """

        if os.path.exists(self.repopath):
            self.pull_bitbucket()
            _updatecommand = self._hg_command[:]
            _updatecommand.extend(['update', '-C', self.branch])
            self.process_command(_updatecommand)
        else:
            self.clone_bitbucket()

    def check_directories(self):
        """
        Check for and create needed packaging directories in repository path
        """

        for directory in self._needed_directories:
            try:
                dirpath = os.path.join(self._parent_repopath, directory)
                os.mkdir(dirpath)
                print "Required Directory {} created".format(dirpath)
            except OSError:
                print "Required Directory {} exists".format(dirpath)

    def cleanup_packaging(self):
        """
        Cleanup unnecessary leftover directories and reset configs
        
        Something like this:

        if os.path.exists(config_temp_path):
            shutil.move(config_temp_path, config_path)
        else:
            os.remove(config_path)

        shutil.rmtree(os.path.join(self._parent_repopath, 'binary', self.scripttime))
        shutil.rmtree(os.path.join(self._parent_repopath, 'sources'))
        """
        
    def package(self):
        """
        Do the actual method calls to create a packaging
        """

        self.check_directories()

        self.update_repository()
        self.config_ssh()

        version = self._changelog_version()
        sourcepath = self.build_dsc(version)
        if self.changelog_action:
            self.act_on_changelog(self.changelog_action, version)

        dscpath = self.find_src_deb(version, sourcepath)
        self.build_deb(dscpath)

        packagebinary = os.path.join(self._parent_repopath, 'binary')
        changesstring = 'openvstorage_{}_amd64.changes'.format(version)
        changespath = os.path.join(packagebinary, changesstring)

        self.upload(changespath)

        self.cleanup_packaging()

if __name__ == '__main__':
    """
    repopath:
        Path to repository for this package
        - should contain a debian subdirectory describing the debian package
    distribution:
        Apt distribution to be used as target upload
        - development, release, revision
    tag:
        Tag to add to version during package creation
        - development string for differentiating between developers (wick, joske, phile..etc)
        - release string for defining pre-release builds (alpha, beta, rc1, rc2..etc)
        - not allowed for official releases or revisions
    branch:
        Branch of repository to update to
        - if not set, default will be used
    promote:
        Promote a Prerelease to this level
    """

    import argparse
    parser = argparse.ArgumentParser(description='OpenvStorage Packager')

    parser.add_argument('-rp', '--repopath', dest='repopath', help='Path to OpenvStorage Repository. Default: /opt/mercurial/openvstorage')
    parser.add_argument('-r', '--repository', dest='repository', help='Bitbucket Mercurial Owner/Repository to use. Default: openvstorage/openvstorage')
    parser.add_argument('-dn', '--distribution', dest='distribution', help='OpenvStorage Apt Repository Distribution: (release | revision | development). Default: revision')
    parser.add_argument('-t', '--tag', dest='tag', help='Tag For Development or Prerelease Builds. Default: None')
    parser.add_argument('-b', '--branch', dest='branch', help='Repository Branch. Default: default')
    parser.add_argument('-p', '--promote', dest='promote', action='store_true' , help='Indicate Change in Release Tag but not Increment Patch Version. Default: False')
    parser.add_argument('-a', '--author', dest='author', help='Author of the Debian Package Default: OVS Automatic Packager <packager@openvstorage.com>')
    parser.add_argument('-s', '--sshcredentials', dest='ssh_credentials', help='apt repository upload user:password or path to ssh private key. Default: ~/.ssh/id_rsa')
    parser.add_argument('-c', '--bbcredentials', dest='bb_credentials', help='bitbucket user:password or path to ssh private key. Default: ~/.ssh/id_rsa')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='Debug mode. Default: False')

    args = parser.parse_args()

    OvsPackaging(args).package()
