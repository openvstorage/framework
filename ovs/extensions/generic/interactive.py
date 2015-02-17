# Copyright 2014 CloudFounders NV
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

import getpass


class Interactive(object):
    """
    This class contains various interactive methods
    """

    @staticmethod
    def ask_integer(question, min_value, max_value, default_value=None, invalid_message=None):
        """
        Asks an integer to the user
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
        """
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
    def ask_string(message='', default_value=None):
        """
        Asks the user a question
        """
        default_string = ': ' if default_value is None else ' [{0}]: '.format(default_value)
        result = raw_input(str(message) + default_string).rstrip(chr(13))
        if not result and default_value is not None:
            return default_value
        return result

    @staticmethod
    def ask_yesno(message='', default_value=None):
        """
        Asks the user a yes/no question
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
            result = raw_input(str(message) + ynstring).rstrip(chr(13))
            if not result and default_value is not None:
                return default_value
            if result.lower() in ('y', 'yes'):
                return True
            if result.lower() in ('n', 'no'):
                return False
            print failuremsg

    @staticmethod
    def ask_password(message=''):
        if message:
            print message
        else:
            print 'Enter the password:'
        return getpass.getpass()

    @staticmethod
    def find_in_list(items, search_string):
        """
        Finds a given string in a list of items
        """
        for item in items:
            if search_string in item:
                return item
        return None

    @staticmethod
    def boxed_message(lines, character='+', maxlength=80):
        """
        Embeds a set of lines into a box
        """
        character = str(character)  # This must be a string
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
        newlines = [character * (maxlen + 10)]
        for line in corrected_lines:
            newlines.append('{0}  {1}{2}  {3}'.format(character * 3, line, ' ' * (maxlen - len(line)),
                                                      character * 3))
        newlines.append(character * (maxlen + 10))
        return '\n'.join(newlines)
