# license see http://www.openvstorage.com/licenses/opensource/
"""
Injector module
"""
from ovs.plugin.injection.loader import Loader


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
        framework = Loader.load(module)
        injector_module = __import__(name='ovs.plugin.injection.injectors.{0}'.format(framework),
                                     globals=globals(),
                                     locals=locals(),
                                     fromlist=['Injector'],
                                     level=0)
        injector = getattr(injector_module, 'Injector')
        inject = getattr(injector, 'inject_{0}'.format(module.__name__.lower()))
        return inject(module)
