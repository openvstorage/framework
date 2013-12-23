"""
Injector module
"""


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
    def inject(framework, modules):
        """
        Injects functionality of a given framework into the given modules
        """
        module = __import__(name='ovs.plugin.injection.injectors.{0}'.format(framework),
                            globals=globals(),
                            locals=locals(),
                            fromlist=['Injector'],
                            level=0)
        injector = getattr(module, 'Injector')
        for module in modules:
            inject = getattr(injector, 'inject_{}'.format(module.__name__.lower()))
            inject(module)
