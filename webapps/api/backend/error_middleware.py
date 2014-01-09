# license see http://www.openvstorage.com/licenses/opensource/
"""
Error middleware module
"""
import traceback
import os


class ExceptionMiddleware(object):
    """
    Error middleware object
    """
    def process_exception(self, request, exception):
        """
        Logs information about the given error to a plain logfile
        """
        _ = request, exception
        # @TODO: Use a real logger instead of raw dumping to a file
        os.system("echo '" + traceback.format_exc() + "' >> /var/log/ovs/django.log")

