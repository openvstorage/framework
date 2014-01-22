"""
PyDev script to trigger pythontidy code formatting on demand
by pressing CTRL + 2 and pt
"""

import tempfile
import os

if False:
    from org.python.pydev.editor import PyEdit
    cmd = 'command string'
    editor = PyEdit

SCRIPT = '/usr/local/bin/PythonTidy'

ACTIVATION_STRING = 'pt'
WAIT_FOR_ENTER = False


class PythonTidyWrapper(object):

    """
    Provides automatic code formatting based on pep8
    """

    def __init__(self):
        self.document = editor.getDocument()
        self.filename = editor.getEditorFile().getName()

    def get_filename(self):
        """return editor filename"""
        return self.filename

    def get_document(self):
        """return editor document"""
        return self.document

    def parse(self):
        """format source file using external PythonTidy command"""

        if not self.document.getNumberOfLines():
            print 'skipping empty source file'
            return
        else:
            tmp_input_file = tempfile.mktemp()
            tmp_output_file = tempfile.mktemp()

            input_file = open(tmp_input_file, 'w')
            text = self.document.get()
            input_file.write(self.document.get())
            input_file.flush()
            input_file.close()

            os.system(SCRIPT + ' %s %s' % (tmp_input_file,
                      tmp_output_file))

            output_file = open(tmp_output_file, 'r')
            tidy_text = output_file.read()
            output_file.close()

            self.document.replace(0, len(text), tidy_text)

            if os.path.exists(tmp_input_file):
                os.remove(tmp_input_file)
            if os.path.exists(tmp_output_file):
                os.remove(tmp_output_file)

        print 'parsing completed...'


if cmd == 'onCreateActions':

    class PythonTidy(Action):

        def __init__(self):
            pass

        def run(self):
            """eclipse action entry point for code formatting"""

            formatter = PythonTidyWrapper()
            formatter.parse()

    editor.addOfflineActionListener(ACTIVATION_STRING, PythonTidy(),
                                    'Format code using pythontidy',
                                    WAIT_FOR_ENTER)
