# Copyright 2016 iNuron NV
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
Interactive extension
Used to interactively ask for an integer, choice, password, ...
"""

import re
import getpass


class Interactive(object):
    """
    This class contains various interactive methods
    Notes:
        .rstrip(chr(13)) only removes \r if it's the last one
        .rstrip() removes all characters including \r, \n, \r\n and trailing whitespace
    """

    @staticmethod
    def ask_integer(question, min_value, max_value, default_value=None, invalid_message=None):
        """
        Asks an integer to the user
        :param question: Question to ask
        :param min_value: Minimum value for integer
        :param max_value: Maximum value for integer
        :param default_value: Default value
        :param invalid_message: Message displayed when entering invalid value
        :return: Specified integer
        """
        if invalid_message is None:
            invalid_message = 'Invalid input please try again.'
        if default_value is not None:
            question = '{0} [{1}]: '.format(question, default_value)
        while True:
            i = raw_input(question).rstrip()
            if i == '' and default_value is not None:
                i = str(default_value)
            if not i.isdigit():
                print invalid_message
            else:
                i = int(i)
                if min_value <= i <= max_value:
                    return i
                else:
                    print invalid_message

    @staticmethod
    def ask_choice(choice_options, question=None, default_value=None, sort_choices=True):
        """
        Lets the user chose one of a set of options
        :param choice_options: Options to choose from
        :param question: Question to ask
        :param default_value: Default value
        :param sort_choices: Sort the specified options
        :return: Option chosen
        """
        if isinstance(choice_options, set):
            choice_options = list(choice_options)

        if not choice_options:
            return None
        if len(choice_options) == 1:
            print 'Found exactly one choice: {0}'.format(choice_options[0])
            return choice_options[0]
        if sort_choices:
            choice_options.sort()
        print '{0}Make a selection please: '.format('{0}. '.format(question) if question is not None else '')
        nr = 0
        default_nr = None
        for section in choice_options:
            nr += 1
            print '    {0}: {1}'.format(nr, section)
            if section == default_value:
                default_nr = nr

        result = Interactive.ask_integer(question='  Select Nr: ',
                                         min_value=1,
                                         max_value=len(choice_options),
                                         default_value=default_nr)
        return choice_options[result - 1]

    @staticmethod
    def ask_string(message='', default_value=None, regex_info=None):
        """
        Asks the user a question
        :param message: Message to show when asking the string
        :param default_value: Default value
        :param regex_info: Dictionary with regex and message when regex does not match
        :return: String
        """
        if regex_info is None:
            regex_info = {}

        if not isinstance(regex_info, dict):
            raise ValueError('Regex info should be a dictionary with "regex" as key and the actual regex as value')

        regex = regex_info.get('regex')
        error_message = regex_info.get('message')
        default_string = ': ' if default_value is None else ' [{0}]: '.format(default_value)
        while True:
            result = raw_input(str(message) + default_string).rstrip()
            if result == '' and default_value is not None:
                result = default_value
            if regex is not None and not re.match(regex, result):
                print error_message if error_message else 'Provided string does not match regex {0}'.format(regex)
                continue
            return result

    @staticmethod
    def ask_yesno(message='', default_value=None):
        """
        Asks the user a yes/no question
        :param message: Message to show when asking yes/no
        :param default_value: Default value
        :return: True or False
        """
        if default_value is None:
            ynstring = ' (y/n): '
            failuremsg = "Illegal value. Press 'y' or 'n'."
        elif default_value is True:
            ynstring = ' ([y]/n): '
            failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
        elif default_value is False:
            ynstring = ' (y/[n]): '
            failuremsg = "Illegal value. Press 'y' or 'n' (or nothing for default)."
        else:
            raise ValueError('Invalid default value {0}'.format(default_value))
        while True:
            result = raw_input(str(message) + ynstring).rstrip()
            if not result and default_value is not None:
                return default_value
            if result.lower() in ('y', 'yes'):
                return True
            if result.lower() in ('n', 'no'):
                return False
            print failuremsg

    @staticmethod
    def ask_password(message=''):
        """
        Ask the user for a password
        :param message: Message to show when asking for a password
        :return: Password specified
        """
        if message:
            print message
        else:
            print 'Enter the password:'
        # Password input should not be rstrip'ed
        return getpass.getpass()

    @staticmethod
    def find_in_list(items, search_string):
        """
        Finds a given string in a list of items
        :param items: Items to search in
        :param search_string: String to search for in items
        :return: Item found or None
        """
        for item in items:
            if search_string in item:
                return item
        return None

    @staticmethod
    def boxed_message(lines, character='+', maxlength=80):
        """
        Embeds a set of lines into a box
        :param lines: Lines to show in the boxed message
        :param character: Character to surround box with
        :param maxlength: Maximum length of a line
        :return:
        """
        corrected_lines = []
        for line in lines:
            if len(line) > maxlength:
                linepart = ''
                for word in line.split(' '):
                    if len(linepart + ' ' + word) <= maxlength:
                        linepart += word + ' '
                    elif len(word) >= maxlength:
                        if len(linepart) > 0:
                            corrected_lines.append(linepart.strip())
                            linepart = ''
                        corrected_lines.append(word.strip())
                    else:
                        if len(linepart) > 0:
                            corrected_lines.append(linepart.strip())
                        linepart = word + ' '
                if len(linepart) > 0:
                    corrected_lines.append(linepart.strip())
            else:
                corrected_lines.append(line)
        maxlen = len(max(corrected_lines, key=len))
        newlines = []
        if character is not None:
            newlines.append(character * (maxlen + 10))
        for line in corrected_lines:
            if character is not None:
                newlines.append('{0}  {1}{2}  {3}'.format(character * 3, line, ' ' * (maxlen - len(line)),
                                                          character * 3))
            else:
                newlines.append(line)
        if character is not None:
            newlines.append(character * (maxlen + 10))
        return '\n'.join(newlines)
