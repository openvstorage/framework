from ovs.lib.helpers.vpool.installers.installer import VPoolInstaller


class CreateInstaller(object, VPoolInstaller):

    def __init__(self, name):
        super(CreateInstaller, self).__init__(name)
        self.is_new = True
