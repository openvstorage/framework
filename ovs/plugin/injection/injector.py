# license see http://www.openvstorage.com/licenses/opensource/
"""
Injector module
"""
import ConfigParser


class Injector(object):
    """
    Injector class, provides all logic to inject
    """

    def __init__(self):
        """
        This class should be fully static
        """
        raise RuntimeError('This class should not be instantiated.')

    @staticmethod
    def inject(module):
        """ Inject module logic and return updated module """
        config = ConfigParser.RawConfigParser()
        config.read('/opt/OpenvStorage/ovs/plugin/injection/settings.cfg')
        if config.has_option('main', 'framework_{0}'.format(module.__name__.lower())):
            framework = config.get('main', 'framework_{0}'.format(module.__name__.lower()))
        else:
            framework = config.get('main', 'framework')
        injector_module = __import__(name='ovs.plugin.injection.injectors.{0}'.format(framework),
                                     globals=globals(),
                                     locals=locals(),
                                     fromlist=['Injector'],
                                     level=0)
        injector = getattr(injector_module, 'Injector')
        inject = getattr(injector, 'inject_{0}'.format(module.__name__.lower()))
        return inject(module)
