#!/usr/bin/env python3

import cowpy 


import unittest
from frank.database.init import setup 
from models import TestieWidgets
import random
logger = cowpy.getLogger()

class TestModel(unittest.TestCase):

    this_name = None 
    this_id = None 

    @classmethod 
    def setUpClass(cls):
        setup()
        cls.this_name = random.randbytes(10).hex()
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        test_widgets = TestieWidgets.get(name=TestModel.this_name)
        for w in test_widgets:
            w.delete()
        return super().tearDownClass()
    
    def setUp(self):
        pass 
        
    def test_001_new(self):        
        tc = TestieWidgets(name=TestModel.this_name)
        self.assertEqual(tc.name, TestModel.this_name)
        self.assertIsNone(tc.id)

    def test_002_save(self):        
        tc = TestieWidgets(name=TestModel.this_name)
        tc.maybe = True 
        tc.data = "{\"this\": \"is\"}"
        tc.counter = 4
        self.assertEqual(tc.name, TestModel.this_name)
        self.assertIsNone(tc.id)
        tc.save()
        TestModel.this_id = tc.id 
        self.assertIsNotNone(tc.id)
        self.assertEqual(tc.counter, 4)
        tc.counter += 1
        tc.save()
        self.assertEqual(tc.id, TestModel.this_id)
    
    def test_003_get(self):
        test_widgets = TestieWidgets.get(name=TestModel.this_name)
        self.assertGreater(len(test_widgets), 0)
        self.assertIn(TestModel.this_id, [ w.id for w in test_widgets ])
    
    def test_004_delete(self):
        id_widgets = TestieWidgets.get(id=TestModel.this_id)
        self.assertEqual(len(id_widgets), 1)
        id_widgets[0].delete()
        id_widgets = TestieWidgets.get(id=TestModel.this_id)
        self.assertEqual(len(id_widgets), 0)

    def test_005_upsert(self):
        upsert_test = TestieWidgets(name='thingama watchy')
        all_widgets = TestieWidgets.all()
        upsert_test.upsert(on='name')
        self.assertNotIn(upsert_test.id, [ w.id for w in all_widgets ])        
        
if __name__ == "__main__":
    unittest.main()