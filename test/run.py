#!/usr/bin/env python3

import os
os.environ['FRANKDB_SETTINGS'] = 'settings'

import unittest
from frank.database.init import setup 
from models import TestClass
import cowpy 
logger = cowpy.getLogger(auto_config=True)

class TestModel(unittest.TestCase):

    def setUp(self):
        setup()

    def test_new(self):
        tc = TestClass(name='test')
        self.assertEqual(tc.name, 'test')

    def test_save(self):
        tc = TestClass(name='test save')
        tc.maybe = True 
        tc.data = {'this': 'is', 'not': 'working'}
        tc.save()
        logger.info(f'tc ID {tc.id}')
        tc.counter += 1
        tc.save()
        
if __name__ == "__main__":
    unittest.main()