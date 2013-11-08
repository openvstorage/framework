#
# This module contains most of the code of stdeb.
#
import re, sys, os, time, tempfile
import ConfigParser, subprocess
from debian.changelog import Changelog, Version
from datetime import datetime
sourceDir = '/var/cache/apt/sources'
archiveDir = '/var/cache/apt/archives'

def _call(*popenargs, **kwargs):
    retcode = subprocess.call(*popenargs, **kwargs)
    if retcode == 0:
        return
    raise RuntimeError(retcode)

def _check_output(*popenargs, **kwargs):
    output = subprocess.check_output(*popenargs, **kwargs)
    return output

def process_command(args, cwd=None):
    if not isinstance(args, (list, tuple)):
        raise RuntimeError, "args passed must be in a list"
    print "Executing %s with CWD %s"%(args, cwd)
    _call(args, cwd=cwd)

def dpkg_source(b_or_x, dsc, options=None, output=None, cwd=None):
    "call dpkg-source [options] -b|x dsc"
    assert b_or_x in ['-b','-x']
    args = ['/usr/bin/dpkg-source',b_or_x, dsc]
    if options:
        args.insert(1, options)
    if output:
        args.append(output)
    process_command(args, cwd=cwd)

def compile_changelog(codeDir, package, version, qualitylevel):
    """
    Parse changelog
    Retrieve last revision from current or previous block and request changelog since that revision
    hg log -b default --rev 62: --template '{date|shortdate} {rev} {desc|firstline}\n' ovs
    2013-10-17 60 The classes in ovs.lib are now called <object>Controller
    """
    def _getLastRevision(changelog):
        regex = re.compile('\s+\*\s+[0-9]+-[0-9]+-[0-9]+\s(?P<rev>[0-9]+)\s(?P<desc>.*)')
        for block in changelog._blocks:
            for change in block.changes():
                m = regex.match(change)
                if m:
                    return m.groupdict()['rev']
        return '1'
    
    def _getCommits(sinceRevision):
        commits = list()
        branch = _check_output(['hg', 'branch']).strip()
        installFile = open('{0}/debian/{1}.install'.format(codeDir, package), 'r')
        for entry in installFile.readlines():
            sd, td = entry.split('')
            args = ['/usr/bin/hg', 'log', '-b', branch, '--prune', sinceRevision, '--template', "'{date|shortdate} {rev} {desc|firstline}\n'", sd]
            commits.append(_check_output(args).splitlines())
        commitSet = set(commits)
        return sorted(commitSet, key=lambda k:str(k).split(' ')[1], reverse=True)
    
    changelog = Changelog()
    changelogFile = '%s/%s.changelog'%(codeDir,package)
    ct = datetime.now()
    cts = ct.strftime('%a, %d %b %Y %H:%M:%S %z') if ct.utcoffset() else '{0} +0000'.format(ct.strftime('%a, %d %b %Y %H:%M:%S'))
    if os.path.exists(changelogFile):
        print "Changelog file exists"
        changelogFileHandler = open(changelogFile, 'r')
        changelogContent = changelogFileHandler.read()
        changelogFileHandler.close()
        try:
            changelog.parse_changelog(changelogContent)
        except:
            pass
    sinceRevision = _getLastRevision(changelog)
    commits = _getCommits(sinceRevision=sinceRevision)
    if not Version(version) in changelog.versions:
        changelog.new_block(package=package,
                            version=Version(version),
                            distributions=qualitylevel,
                            urgency='low',
                            author='Stefaan Aelbrecht <stefaan.aelbrecht@cloudfounders.com>',
                            date=cts)
    for commit in commits:
        changelog.add_change('   * {0})'.format(commit))
    changelogFileHandler = open(changelogFile, 'w')
    changelog.write_to_open_file(changelogFileHandler)
    changelogFileHandler.close()

def build_dsc(codeDir, package, version, qualitylevel):
    """
    @param codeDir: Absolute path to code directory to create debian source package from(should contain a debian subdirectory describing the debian package)
    @
    """
    packages = []
    if not os.path.exists(sourceDir):
        os.mkdir(sourceDir)
    if not os.path.isabs(codeDir):
        raise ValueError('Path to code directory needs to be absolute')
    cwd = os.path.abspath(os.curdir)
    os.chdir(codeDir)
    debianFiles = os.listdir('{0}/debian'.format(codeDir))
    for file in debianFiles:
        filename, ext =  os.path.splitext(file)
        if ext == '.install':
            packages.append(filename)
    for package in packages:
        compile_changelog(codeDir, package, version, qualitylevel)
    
    excludePatterns = ['.hg', '.project', '.settings', '.hgignore']
    excludeArgs = ''
    for p in excludePatterns:
        excludeArgs += '-I%s ' %p
    os.chdir(sourceDir)
    dpkg_source('-b', codeDir, excludeArgs)
    os.chdir(cwd)

def list_dsc():
    dscFiles = []
    for fd in os.listdir(sourceDir):
        filename, ext = os.path.splitext(fd)
        if ext == '.dsc': dscFiles.append(fd)
    return dscFiles

def dpkg_buildpackage(tmpDir, package, cwd=None):
    """
    @param destPath: Destination path 
    @param package: Name of the package to build
    """
    os.chdir(tmpDir)
    args = ['/usr/bin/dpkg-buildpackage','-rfakeroot','-uc','-b','-T', package]
    process_command(args)
    if cwd:
        os.chdir(cwd)

def build_deb(sourceDeb, package):
    """
    @param sourceDeb: Name of the debian source file(*.dsc)
    """
    sourceDebPath = '{0}{1}{2}'.format(sourceDir, os.sep, sourceDeb)
    if not os.path.exists(sourceDebPath):
        raise ValueError('Source package %s not found'%sourceDeb)
    tmpDir = '{0}{1}{2}_{3}'.format(tempfile.gettempdir(), os.sep, package, str(int(time.time())))
    os.chdir(sourceDir)
    dpkg_source('-x', sourceDebPath, output=tmpDir)
    cwd = os.path.abspath(os.curdir)
    os.chdir(archiveDir)
    dpkg_buildpackage(tmpDir, package)
    os.chdir(cwd)

def build_from_repo(url, user, passowrd):
    pass

def upload(deb, url, user, passwd):
    pass