from model import Model
from brander import Brander


class Migration():
    def __init__(self):
        pass

    @staticmethod
    def migrate(previous_version):
        Model.migrate(previous_version)
        Brander.migrate(previous_version)