#!/usr/bin/env python
import re
import os
import tempfile
import subprocess
from debian.changelog import Changelog, Version
from datetime import datetime
from time import time

sourcedir = '/var/cache/apt/sources'
archivedir = '/var/cache/apt/archives'
author = 'OVS Automatic Packager <packager@openvstorage.com>'
ct = datetime.now()
if ct.utcoffset():
    cts = ct.strftime('%a, %d %b %Y %H:%M:%S %z')
else:
    cts = '{} +0000'.format(ct.strftime('%a, %d %b %Y %H:%M:%S'))
scripttime = int(time())

def _call(*popenargs, **kwargs):
    retcode = subprocess.call(*popenargs, **kwargs)
    if retcode == 0:
        return
    raise RuntimeError(retcode)

def _check_output(*popenargs, **kwargs):
    output = subprocess.check_output(*popenargs, **kwargs)
    return output

def _increment_version(versionnumber, increment):
    """
    Increments a <major>.<minor>.<patch> version number as desired
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

def _gather_version(repopath, incrementversion):
    """
    extracts version from repository changelog
    returns incremented part of version if indicated or just version
    
    """
    changelog = Changelog()
    changelogfile = os.path.join(repopath, 'debian', 'changelog')
    changelogfilehandler = open(changelogfile, 'r') 

    changelog.parse_changelog(changelogfilehandler)
    currentversion = changelog.full_version.split('~')[0]
    if incrementversion:
        version = _increment_version(currentversion, 'patch')
    else:
        version = currentversion

    return version

def process_command(args, cwd=None):
    if not isinstance(args, (list, tuple)):
        raise RuntimeError, 'args passed must be in a list'
    print 'Executing {} with CWD {}'.format(args, cwd)
    _call(args, cwd=cwd)

def dpkg_source(b_or_x, dsc, options=None, output=None, cwd=None):
    'call dpkg-source [options] -b|x dsc'
    assert b_or_x in ['-b', '-x']
    args = ['/usr/bin/dpkg-source', b_or_x, dsc]
    if options:
        args.insert(1, options)
    if output:
        args.append(output)
    process_command(args, cwd=cwd)

def clone_bitbucket(credentials, repository, repodir, branch):
    """
    """
    if branch != 'default':
        clonecommand = ['hg', '-yq', '--cwd', repodir, 'clone', '-u', branch]
    else:
        clonecommand = ['hg', '-yq', '--cwd', repodir, 'clone']

    if os.path.exists(credentials):
        sshcommand = 'ssh -i {} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'.format(credentials)
        sshurl = 'ssh://hg@bitbucket.org/openvstorage/{}'.format(repository)
        clonecommand.extend(['-e', sshcommand, sshurl])
    else:
        clonecommand.append('https://{}@bitbucket.org/openvstorage/{}'.format(credentials, repository))

    process_command(clonecommand)

def push_bitbucket(credentials, repopath):
    """
    """
    pushcommand = ['hg', '-yq', '--cwd', repopath, 'push']

    repository = os.path.split(repopath)[1]
    if os.path.exists(credentials):
        sshcommand = 'ssh -i {} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'.format(credentials)
        sshurl = 'ssh://hg@bitbucket.org/openvstorage/{}'.format(repository)
        pushcommand.extend(['-e', sshcommand, sshurl])
    else:
        pushcommand.append('https://{}@bitbucket.org/openvstorage/{}'.format(credentials, repository))

    process_command(pushcommand)

def add_version_to_changelog(repopath, version, qualitylevel):
    """
    add new version to main source changelog
    """

    currentversion = Version(version)
    changelogfile = os.path.join(repopath, 'debian', 'changelog')
    changelogfilehandler = open(changelogfile, 'r')

    changelog = Changelog()
    changelog.parse_changelog(changelogfilehandler)
    if not currentversion in changelog.versions:
        changelog.new_block(
                package='openvstorage',
                version=currentversion,
                distributions='ovs-{}'.format(qualitylevel),
                urgency='low',
                author=author,
                date=cts)

    change = '\n   * CloudFounders {} Open vStorage release '
    change += '( see changelog for more info on changes )\n'
    changelog.add_change(change.format(version))

    changelogfilehandler = open(changelogfile, 'w')
    changelog.write_to_open_file(changelogfilehandler)
    changelogfilehandler.close()

def act_on_changelog(repopath, changelog_action, version, credentials):
    """
    """
    if changelog_action == 'commit':
        changelogcommand = ['hg', '-yq', '--cwd', repopath, changelog_action, '-m ovs release {}'.format(version)]
        process_command(changelogcommand)
        push_bitbucket(credentials, repopath)
    elif changelog_action == 'revert':
        changelogpath = os.path.join(repopath, 'debian', 'changelog')
        changelogcommand = ['hg', '-yq', changelog_action, '-a', changelogpath]
        process_command(changelogcommand, cwd=repopath)
    else:
        return 1
    
def compile_changelog(codedir, package, version, qualitylevel):
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
        branch = _check_output(['hg', 'branch']).strip()
        installfile = open('{}/debian/{}.install'.format(codedir, package), 'r')
        for entry in installfile.readlines():
            sd = entry.split(' ')[0]
            args = ['/usr/bin/hg', 'log', '-b', branch, '--prune', sincerevision, '--template', '{date|shortdate} {rev} {desc|firstline}\n', sd]
            commits.extend(_check_output(args).splitlines())
        commitset = set(commits)
        return sorted(commitset, key=lambda k:str(k).split(' ')[1], reverse=True)
    
    changelog = Changelog()
    changelogfile = '{}/{}.changelog'.format(codedir, package)
    if os.path.exists(changelogfile):
        print 'Changelog file exists'
        changelogfilehandler = open(changelogfile, 'r')
        try:
            changelog.parse_changelog(changelogfilehandler)
        except:
            pass
    sincerevision = _get_last_revision(changelog)
    commits = _getcommits(sincerevision=sincerevision)
    if not Version(version) in changelog.versions:
        print 'adding new version to changelog {}'.format(version)
        changelog.new_block(package=package,
                            version=Version(version),
                            distributions=qualitylevel,
                            urgency='low',
                            author=author,
                            date=cts)
    if commits:
        for commit in commits:
            changelog.add_change('   * {})'.format(commit))
    else:
        changelog.add_change('   *  No Commits')
    changelogfilehandler = open(changelogfile, 'w')
    changelog.write_to_open_file(changelogfilehandler)
    changelogfilehandler.close()

def build_dsc(repopath, qualitylevel, tag, credentials=None, incrementversion=False):
    """
    create the source deb
    """

    abscodedir = os.path.abspath(repopath)
    if not os.path.exists(abscodedir):
        raise OSError('Code directory {} must exist to continue'.format(abscodedir))

    versionnumber = _gather_version(repopath, incrementversion)
    changelog_action = None
    if qualitylevel == 'development':
        version = '{}~{}~{}'.format(versionnumber, scripttime, tag)
        add_version_to_changelog(repopath, version, qualitylevel)
        # changelog_action = 'revert'
    elif tag and qualitylevel == 'release': 
        patchnumber = versionnumber.split('.')[2]
        versionnumber.split('.')[2] = int(patchnumber) + 1
        newversion = '.'.join(versionnumber)
        version = '{}~{}'.format(newversion, tag)
        add_version_to_changelog(repopath, version, qualitylevel, credentials)
        changelog_action = 'commit'
    else:
        version = versionnumber

    packages = list()
    cwd = os.path.abspath(os.curdir)
    package = os.path.basename(repopath)

    if not os.path.exists(sourcedir):
        os.mkdir(sourcedir)

    os.chdir(abscodedir)
    debianfiles = os.listdir('{}/debian'.format(abscodedir))
    for debfile in debianfiles:
        filename, ext =  os.path.splitext(debfile)
        if ext == '.install':
            packages.append(filename)

    for package in packages:
        print 'packaging {} with version {}'.format(package, version)
        compile_changelog(repopath, package, version, qualitylevel)
    
    exclude_patterns = ['.hg', '.project', '.settings', '.hgignore']
    exclude_args = ''
    for pattern in exclude_patterns:
        exclude_args += '-I{} '.format(pattern)
    os.chdir(sourcedir)
    dpkg_source('-b', repopath, exclude_args)

    if changelog_action:
        act_on_changelog(repopath, changelog_action, version, credentials)

    os.chdir(cwd)

    srcdebpath = None

    srcdebdir = os.path.split(repopath)[0]
    for srcdeb in os.listdir(srcdebdir):
        print 'found {} looking for file ending with "{}.dsc" in {}'.format(srcdeb, version, srcdebdir)
        if srcdeb.endswith('{}.dsc'.format(version)):
            srcdebpath = os.path.join(srcdebdir, srcdeb)

    if not srcdebpath:
        raise RuntimeError('Source Debian Package Failure - Source Deb Not Found')

    print 'source deb file path: {}'.format(srcdebpath)
    return srcdebpath

def list_dsc():
    dscfiles = list() 
    for fd in os.listdir(sourcedir):
        filename, ext = os.path.splitext(fd)
        if ext == '.dsc':
            dscfiles.append(fd)
    return dscfiles

def dpkg_buildpackage(tmpdir, package, cwd=None):
    """
    @param destPath: Destination path 
    @param package: Name of the package to build
    """
    os.chdir(tmpdir)
    if package:
        args = ['/usr/bin/dpkg-buildpackage', '-rfakeroot', '-uc', '-us', '-b', '-T', package]
    else:
        args = ['/usr/bin/dpkg-buildpackage', '-rfakeroot', '-uc', '-us']
    process_command(args)
    if cwd:
        os.chdir(cwd)

def build_deb(sourcedebpath, package=None):
    """
    @param sourcedeb: Name of the debian source file(*.dsc)
    """
    packagetmp = sourcedebpath.split('_')[0]
    if not os.path.exists(sourcedebpath):
        raise ValueError('Source package {} not found'.format(sourcedebpath))
    tmpdir = os.path.join(tempfile.gettempdir(), '{}_{}'.format(packagetmp, scripttime))
    os.chdir(sourcedir)
    dpkg_source('-x', sourcedebpath, output=tmpdir)
    cwd = os.path.abspath(os.curdir)
    os.chdir(archivedir)
    dpkg_buildpackage(tmpdir, package)
    os.chdir(cwd)
    
def update_repository(repository, credentials, branch):
    """
    default branch = unstable
    credentials: 
        string
         - user:pass or path to private key
    """
    repodir = os.path.join(os.sep, 'opt', 'mercurial')
    if not os.path.exists(repodir):
        os.mkdir(repodir)

    repopath = os.path.join(repodir, repository)
    if os.path.exists(repopath):
        updatecommand = ['hg', '-yq', '--cwd', repopath, 'update', '-C', branch]
        process_command(updatecommand)
    else:
        clone_bitbucket(credentials, repository, repodir, branch)

def upload(changesfile, credentials):
    """
    uploads deb to apt server using dput
    - assumes a correct working dput.cf
    - dput.cf to be created at a later date
    credentials: user:pass or path to private key
    """

    # TODO: copy sshconfig, write a new one, and move the old one back after success
    def _writesshconfig(addconfig):
        easykeylist = ['UserKnownHostsFile /dev/null\n', 'StrictHostKeyChecking no\n']
        sshconfig = os.path.expanduser('~/.ssh/config')
        if not os.path.exists(sshconfig):
            configf = open(sshconfig, 'w+')
        else:
            configf = open(sshconfig, 'r+')
        configlines = configf.readlines()
        for easykey in easykeylist:
            if easykey not in configlines:
                configf.write(easykey)
        if addconfig not in configlines:
            configf.write(addconfig)
        configf.close()


    dputcommand = ['dput', '-u', 'ovs', changesfile]

    print 'dput command:\n{}'.format(dputcommand)

    if os.path.exists(credentials):
        _writesshconfig('IdentityFile {}'.format(credentials))
        process_command(dputcommand)
    else:
        splitcreds = credentials.split(':')
        username = splitcreds[0]
        password = ''.join(splitcreds[1:])
        _writesshconfig('User {}\n'.format(username))
        import pexpect
        pexpect.run(' '.join(dputcommand), events={'(?i)password:': '{}\n'.format(password)})

if __name__ == '__main__':
    """
    repopath: 
        Path to repository for this package
        - should contain a debian subdirectory describing the debian package
    qualitylevel: 
        Level of quality to determine distribution from
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
    parser.add_argument('-r', '--repopath', dest='repopath', required=True, help='Path to OpenvStorage Repository')
    parser.add_argument('-q', '--qualitylevel', dest='qualitylevel', required=True, help='OpenvStorage Quality level: (release | revision | development)')
    parser.add_argument('-t', '--tag', dest='tag', help='Tag For Development or Prerelease Builds')
    parser.add_argument('-s', '--sshcredentials', dest='ssh_credentials', required=True, help='apt repository upload user:password or path to ssh private key')

    parser.add_argument('-b', '--branch', dest='branch', help='Repository Branch')
    parser.add_argument('-c', '--bbcredentials', dest='bb_credentials', help='bitbucket user:password or path to ssh private key')
    parser.add_argument('-p', '--promote', dest='promote', help='Indicate Change in Release Tag but not Increment Patch Version')
    # instead of passing url, create a dput.cf file
    # maybe can create from a url.. maybe need it out of this script
    # parser.add_argument('-u', '--url', dest='url', help='destination upload apt repository ftp:// or scp://')

    parser.add_argument('-n', '--non-interactive', dest='noninteractive', action='store_true', help='Non-interactive mode')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='Debug mode')

    args = parser.parse_args()

    repopath = args.repopath
    quality = args.qualitylevel
    tag = args.tag

    incrementversion = False
    if (args.tag and args.qualitylevel == 'release') or args.qualitylevel == 'development':
        incrementversion = True

    if (args.branch or incrementversion):
        if args.bb_credentials:
            credentials = args.bb_credentials
        else:
            raise AttributeError('Bitbucket credentials necessary')

    if args.branch:
        update_repository(repopath, args.branch, credentials)

    if incrementversion:
        dscpath = build_dsc(repopath, quality, tag, credentials, incrementversion)
    else:
        dscpath = build_dsc(repopath, quality, tag)

    build_deb(dscpath)
    changesfile = '{}_amd64.changes'.format(dscpath[:-4])
    upload(changesfile, args.ssh_credentials)
