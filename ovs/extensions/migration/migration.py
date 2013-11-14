"""
Migration module
"""
from model import Model
from brander import Brander


class Migration():
    """
    This class is an entry point for the migration
    """

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version):
        """
        Executes the migration
        """
        Model.migrate(previous_version)
        Brander.migrate(previous_version)
