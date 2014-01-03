# license see http://www.openvstorage.com/licenses/opensource/
"""
precommit code validation script
"""

import argparse
import difflib
import fnmatch
import os
import re
import tempfile
import time

# from closure_linter import errors as closure_linter_errors
# from closure_linter import errorrules
# from closure_linter import runner
# from closure_linter.common.erroraccumulator import ErrorAccumulator

from ConfigParser import ConfigParser

REGEX_OPTIONS = re.IGNORECASE | re.MULTILINE | re.DOTALL
CRLF_PATTERN = re.compile(r'\r\n$', REGEX_OPTIONS)
PDB_PATTERN = re.compile(r'(?!#)*pdb\.set_trace', REGEX_OPTIONS)

PYTHONTIDY_CMD = 'PythonTidy'
PYLINT_CMD = 'pylint'


# def monkey_patch_closure():
#     ORIGINAL_SHOULD_REPORT_ERROR = None
#
#     def inject_error_reporter():
#         global ORIGINAL_SHOULD_REPORT_ERROR
#         ORIGINAL_SHOULD_REPORT_ERROR = errorrules.ShouldReportError
#         errorrules.ShouldReportError = custom_should_report_error
#
#     def custom_should_report_error(error):
#         global ORIGINAL_SHOULD_REPORT_ERROR
#         return error not in (closure_linter_errors.MULTI_LINE_STRING,) \
#             and ORIGINAL_SHOULD_REPORT_ERROR(error)

#    inject_error_reporter()

# monkey patch for closure linter

# monkey_patch_closure()

def get_repo_dir():
    """
    Gets the absolute path of the repository directory.

    :return: absolute path of the repository directory
    :rtype: str
    """

    rel_dir = os.path.dirname(__file__)
    repo_rel_dir = os.path.join(rel_dir, '..', '..')
    return os.path.abspath(repo_rel_dir)


def get_errors_with_pattern(pattern, code, error_base):
    """
    Checks for errors with a specific pattern in a piece of code.

    :param pattern: pattern of the error to check for
    :type pattern: str

    :param code: code to check for errors
    :type code: str

    :param error_base: error base message
    :type error_base: str
    """

    line_numbers = list()

    for match in re.finditer(pattern, code):
        line_number = code.count(os.linesep, 0, match.start()) + 1
        line_numbers.append(str(line_number))

    if not line_numbers:
        return list()

    if len(line_numbers) > 1:
        template = '%s (lines %s)'
    else:
        template = '%s (line %s)'

    return [template % (error_base, ', '.join(line_numbers))]


def get_crlf_errors(code):
    """
    Checks a piece of code for CRLF (Carriage Return Line Feed) entries.

    :param code: code to check
    :type: str

    :return: errors found in the code
    :rtype: list
    """

    return get_errors_with_pattern(CRLF_PATTERN, code,
                                   'found crlf entry')


def get_pdb_errors(code):
    """
    Checks a piece of code for pdb (Python debugger) entries.

    :param code: code to check
    :type: str

    :return: errors found in the code
    :rtype: list
    """

    return get_errors_with_pattern(PDB_PATTERN, code, 'found pdb entry')


def verify_pythontidy_parsing(python_file):
    """
    validate code has been formatted before allowing commit
    """

    tmp_output_file = tempfile.mktemp()
    os.system(PYTHONTIDY_CMD + ' %s %s' % (python_file,
              tmp_output_file))

    fromdate = time.ctime(os.stat(python_file).st_mtime)
    todate = time.ctime(os.stat(tmp_output_file).st_mtime)
    with open(python_file, 'U') as input_file:
        fromlines = input_file.readlines()
    with open(tmp_output_file, 'U') as output_file:
        tolines = output_file.readlines()
    diff = difflib.unified_diff(
        fromlines,
        tolines,
        python_file,
        tmp_output_file,
        fromdate,
        todate,
        n=2,
        )

    os.remove(tmp_output_file)

    return diff


def get_pylint_errors_and_warnings(python_file):
    """
    parse source file using pylint and return parseable result
    """

    tmp_output_file = tempfile.mktemp()
    os.system(PYLINT_CMD + ' -E -r n - %s > %s' % (python_file,
              tmp_output_file))

    return ''


def get_python_files(target_dir):
    """
    Gets the paths of all Python files in a target directory.

    :param target_dir: directory to search
    :type targets_dir: str

    :return: Python file paths
    :rtype: str
    """

    python_files = list()

    for (root, _, file_names) in os.walk(target_dir):
        for python_file_name in fnmatch.filter(file_names, '*.py'):
            python_file = os.path.join(root, python_file_name)
            python_files.append(python_file)

    return python_files


def get_common_errors(target_file):
    """
    Checks a target file for errors that any type of text based file can
    contain.

    :param target_dir: directory to check
    :type target_dir: str

    :return: found errors
    :rtype: list
    """

    with open(target_file) as target:
        code = target.read()

    return get_crlf_errors(code)


def get_python_errors(target_file):
    """
    Checks a target Python file for errors that only a Python file can
    contain.

    :param target_dir: directory to check
    :type target_dir: str

    :return: found errors
    :rtype: list
    """

    errors = list()
    errors.extend(verify_pythontidy_parsing(target_file))
    errors.extend(get_pylint_errors_and_warnings(target_file))
    code = open(target_file).read()
    errors.extend(get_pdb_errors(code))

    return errors


def is_file_ignored(file_name):
    """
    filetype to be ignored during precommit validation
    """

    ignored_file_name_ends = [
        '.eot',
        '-min.js',
        '.min.js',
        '.pyc',
        '.svg',
        '.ttf',
        '.woff',
        ]
    for file_name_end in ignored_file_name_ends:
        if file_name.endswith(file_name_end):
            return True

    return False


def get_report(target_dir, changes_only=False):
    """
    Performs error checks on a target dir and returns a report.

    :param target_dir: directory to check
    :type target_dir: str

    :return: report of the found errors
    :rtype: list
    """

    report = dict()
    files_to_process = list()
    if changes_only:
        tmp_output_file = tempfile.mktemp()
        os.system("cd %s;hg stat -m -a  %s | awk '{print $2}' > %s"
                  % (target_dir, target_dir, tmp_output_file))
        with open(tmp_output_file, 'r') as output_file:
            for line in output_file.readlines():
                files_to_process.append((target_dir, line.strip()))
        os.remove(tmp_output_file)
    else:
        for (root, sub_dirs, file_names) in files_to_process:
            if '.hg' in sub_dirs:
                sub_dirs.remove('.hg')

            for file_name in file_names:
                files_to_process.append((root, file_name))

    for (root, file_name) in files_to_process:
        if root.endswith('/clouddesktop-ui/scripts/rdp-client') \
            or root.endswith('/clouddesktop-ui/scripts/pdfjs') \
            or root.endswith('/migrations') \
            or is_file_ignored(file_name):
            continue

        target_file = os.path.join(root, file_name)
        errors = get_common_errors(target_file)

        if file_name.endswith('.py'):
            print file_name
            python_errors = get_python_errors(target_file)
            errors.extend(python_errors)

        if errors:
            report[target_file] = errors

    return report


def make_report_printable(report):
    """
    Makes a error report printable.

    :param report: reported errors
    :type report: list

    :return: printable report
    :rtype: str
    """

    report_pieces = list()

    for (target_file, errors) in report.iteritems():
        lines = list()

        lines.append('%s' % target_file)

        for error in errors:
            lines.append('  * %s' % error)

        report_piece = os.linesep.join(lines)
        report_pieces.append(report_piece)

    return (os.linesep * 2).join(report_pieces)


def check_repo(changes_only=False):
    """
    Checks all files in the repository for errors and returns a report
    when erros are found.

    :returns: report of the files containing errors
    :rtype: dict
    """

    repo_dir = get_repo_dir()
    report = get_report(repo_dir, changes_only)

    return make_report_printable(report)


def install_mercurial_hook():
    """
    Installs the mercurial precommit hook by adding a hook to the hgrc
    file in the .hg directory of the repository.
    """

    repo_dir = get_repo_dir()

    config_file = os.path.join(repo_dir, '.hg', 'hgrc')
    config_parser = ConfigParser()
    config_parser.read(config_file)

    precommit_abs_file = os.path.join(repo_dir, 'scripts',
            'codestyleprecommit.py')

    section = 'hooks'
    key = 'pretxncommit.precommit'
    value = 'python:%s:mercurial_hook' % precommit_abs_file

    if not config_parser.has_section(section):
        config_parser.add_section(section)

    config_parser.set(section, key, value)

    with open(config_file, 'w') as config:
        config_parser.write(config)


def mercurial_hook(ui, repo, **kwargs):
    """
    Checks all files in the repository for errors and prints a report
    when errors are found. This function can be used by mercurial as a
    hook.

    :return: True when the check failed, False otherwise
    :rtype: bool
    """

    report = check_repo(changes_only=True)

    if report:
        print report
        return True  # Failure
    else:
        return True  # Success


if __name__ == '__main__':
    parser = \
        argparse.ArgumentParser(description='Checks all files in the repository for errors and prints a report when errors are found.'
                                )

    parser.add_argument('-i', '--install', action='store_true',
                        dest='install',
                        help='Installs the mercurial precommit hook by adding a hook entry to the hgrc file in the .hg directory of the repository.'
                        )

    args = parser.parse_args()

    if args.install:
        install_mercurial_hook()
    else:
        print check_repo(True)
