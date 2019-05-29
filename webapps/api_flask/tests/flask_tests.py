import sys
import unittest

sys.path.append('/opt/OpenvStorage/webapps')
import api_flask

class FlaskTestCase(unittest.TestCase):
    """
    Set up the flask server in testing mode. Used for every unittest to provide an easy inheritable mock
    """
    def setUp(self):
        api_flask.app.testing = True
        self.app = api_flask.app.test_client()

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()