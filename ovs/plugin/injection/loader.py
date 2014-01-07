# license see http://www.openvstorage.com/licenses/opensource/
"""
Framework loader module
"""


class Loader(object):
    """
    Loader class
    """

    def __init__(self):
        """
        Empty constructor
        """
        pass

    @staticmethod
    def load(module):
        import ConfigParser
        config = ConfigParser.RawConfigParser()
        config.read('/opt/OpenvStorage/ovs/plugin/injection/settings.cfg')
        if config.has_option('main', 'framework_{0}'.format(module.__name__.lower())):
            framework = config.get('main', 'framework_{0}'.format(module.__name__.lower()))
        else:
            framework = config.get('main', 'framework')
        return framework
