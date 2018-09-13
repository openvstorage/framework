class GenericConfig():

    def get_config(self):
        return vars(self)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            if vars(self) == vars(other):
                return True
        return False

    def __ne__(self, other):
        if isinstance(other, type(self)):
            if vars(self) == vars(other):
                return False
        return True