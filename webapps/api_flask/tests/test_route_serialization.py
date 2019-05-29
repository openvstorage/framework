import json
from .flask_tests import FlaskTestCase
from ovs.dal.tests.helpers import DalHelper
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool


class StorageRouterTest(FlaskTestCase):

    def setUp(self):
        super(StorageRouterTest, self).setUp()
        self.structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1), (2, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <storagerouter_id>)
        )
        self.sr = self.structure['storagerouters'][1]
        self.vd = self.structure['vdisks'][1]
        self.vp = self.structure['vpools'][1]
        self.sr_properties = [i.name for i in self.sr._properties]
        self.sr_dynamics = [i.name for i in self.sr._dynamics]
        self.vd_properties = [i.name for i in self.vd._properties]
        self.vd_relations = [i.name for i in self.vd._relations]

    def tearDown(self):
        DalHelper.teardown()

    def test_easy_list(self):
        rv = json.loads(self.app.get('/storagerouters/').data)
        out = {u'data': [u'{0}'.format(self.sr.guid)]}
        self.assertDictEqual(out, rv)

    def test_retrieve_simple(self):
        rv_simple = json.loads(self.app.get('/storagerouters/{0}'.format(self.sr.guid)).data)
        for property in self.sr_properties:
            self.assertIn(property, rv_simple)
        for dynamic in self.sr_dynamics:
            self.assertNotIn(dynamic, rv_simple)

    def test_retrieve_all_dynamics(self):
        rv_dynamics = json.loads(self.app.get('/storagerouters/{0}?contents=_dynamics'.format(self.sr.guid)).data)
        for property in self.sr_properties:
            self.assertIn(property, rv_dynamics)
        for dynamic in self.sr_dynamics:
            self.assertIn(dynamic, rv_dynamics)

    def test_retrieve_specific_dynamics(self):
        present_dynamics = ['status', 'vdisks_guids']
        absent_dynamics = [dyn.name for dyn in StorageRouter._dynamics if dyn.name not in present_dynamics]
        rv_dynamics = json.loads(self.app.get('/storagerouters/{0}?contents={1}'.format(self.sr.guid, ','.join(present_dynamics))).data)
        for property in self.sr_properties:
            self.assertIn(property, rv_dynamics)
        for dynamic in present_dynamics:
            self.assertIn(dynamic, rv_dynamics)
        for dynamic in absent_dynamics:
            self.assertNotIn(dynamic, rv_dynamics)

    def test_retrieve_all_but_specific_dynamics(self):
        absent_dynamics = ['status', 'vdisks_guids']
        present_dynamics = [dyn.name for dyn in StorageRouter._dynamics if dyn.name not in absent_dynamics]
        rv_dynamics = json.loads(self.app.get('/storagerouters/{0}?contents=_dynamics,-{1}'.format(self.sr.guid, ',-'.join(absent_dynamics))).data)
        for property in self.sr_properties:
            self.assertIn(property, rv_dynamics)
        for dynamic in absent_dynamics:
            self.assertNotIn(dynamic, rv_dynamics)
        for dynamic in present_dynamics:
            self.assertIn(dynamic, rv_dynamics)

    def test_retrieve_relations(self):
        rv_relations = json.loads(self.app.get('/vdisks/{0}?contents=_relations'.format(self.vd.guid)).data)
        for property in self.vd_properties:
            self.assertIn(property, rv_relations)
        for relation in self.vd_relations:
            relation_key = '{0}_guid'.format(relation)  #depth = 0, so only guids are provided
            self.assertIn(relation_key, rv_relations)

    def test_retrieve_depth_relations(self):
        rv_relation_depth = json.loads(self.app.get('/vdisks/{0}?contents=_relations,_relations_depth=1'.format(self.vd.guid)).data)
        for relation in self.vd_relations:
            # depth = 1, so entire dataobject is serialized with keys being 'parent_vdisk' and 'vpool'
            self.assertIn(relation, rv_relation_depth)

    def test_retrieve_relations_contents(self):
        wanted_dynamics = ['identifier']
        absent_dynamics = [dyn.name for dyn in VPool._dynamics if dyn.name not in wanted_dynamics]
        present_dynamics = [dyn.name for dyn in VPool._dynamics if dyn.name in wanted_dynamics]
        res = self.app.get('/vdisks/{0}?contents=_relations,_relations_depth=1,_relations_contents="_dynamics,-{1}"'.format(self.vd.guid,',-'.join(absent_dynamics))).data
        rv_relation_depth = json.loads(res)
        for relation in self.vd_relations:
            # depth = 1, so entire dataobject is serialized with keys being 'parent_vdisk' and 'vpool'
            self.assertIn(relation, rv_relation_depth)
        self.assertIn('vpool', rv_relation_depth)
        vp = rv_relation_depth['vpool']
        for dynamic in present_dynamics:
            self.assertIn(dynamic, vp)
        for dynamic in absent_dynamics:
            self.assertNotIn(dynamic, vp)

    def test_retrieve_relations_contents_reuse(self):
        vpool_properties = [prop.name for prop in VPool._properties if prop.name != 'password']
        vpool_dynamics = [dyn.name for dyn in VPool._dynamics]
        res = self.app.get('/vdisks/{0}?contents=relations_contents=re-use'.format(self.vd.guid)).data
        rv_relation_depth = json.loads(res)
        for relation in self.vd_relations:
            # depth = 1, so entire dataobject is serialized with keys being 'parent_vdisk' and 'vpool'
            self.assertIn(relation, rv_relation_depth)
        self.assertIn('vpool', rv_relation_depth)
        vp = rv_relation_depth['vpool']
        for prop in vpool_properties:
            self.assertIn(prop, vp)
        for dyn in vpool_dynamics:
            self.assertNotIn(dyn, vp)


    def test_retrieve_relation_contents_vpool(self):
        vpool_properties = [prop.name for prop in VPool._properties if prop.name != 'password']
        vpool_dynamics = [dyn.name for dyn in VPool._dynamics]
        res = self.app.get('/vdisks/{0}?contents=relation_contents_vpool'.format(self.vd.guid)).data
        rv_relation_depth = json.loads(res)
        for relation in self.vd_relations:
            # depth = 1, so entire dataobject is serialized with keys being 'parent_vdisk' and 'vpool'
            self.assertIn(relation, rv_relation_depth)
        self.assertIn('vpool', rv_relation_depth)
        vp = rv_relation_depth['vpool']
        for prop in vpool_properties:
            self.assertIn(prop, vp)
        for dyn in vpool_dynamics:
            self.assertNotIn(dyn, vp)
