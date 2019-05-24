import json
from .flask_tests import FlaskTestCase
from ovs.dal.tests.helpers import DalHelper


class StorageRouterTest(FlaskTestCase):

    def _build_structure_easy(self):
        return DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
    def test_easy_list(self):
        structure = self._build_structure_easy()
        sr = structure['storagerouters'][1]
        rv = json.loads(self.app.get('/storagerouters/').data)
        out = {u'_contents': None,
               u'_paging': {u'current_page': 1,
                            u'end_number': 1,
                            u'max_page': 1,
                            u'page_size': 1,
                            u'start_number': 1,
                            u'total_items': 1},
               u'_sorting': [u'name'],
               u'data': [u'{0}'.format(sr.guid)]}

        self.assertDictEqual(out, rv)

    def test_list_content(self):
        structure = self._build_structure_easy()
        sr = structure['storagerouters'][1]
        rv = json.loads(self.app.get('/storagerouters/?contents=_dynamics').data)

    def test_easy_retrieve(self):
        structure = self._build_structure_easy()
        sr = structure['storagerouters'][1]

        rv = json.loads(self.app.get('/storagerouters/{0}?contents=_dynamics'.format(sr.guid)).data)
        print rv