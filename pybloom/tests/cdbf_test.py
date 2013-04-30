import sys, os.path
sys.path.append(os.path.split(os.path.abspath(__file__))[0] + '/..')

import unittest
import csv
import time
import random
import datetime
import numpy as np

from cdbf import CountdownBloomFilter, ScalableCountdownBloomFilter


class CountdownBloomFilterTests(unittest.TestCase):
    '''
    Tests for CountdownBloomFilter
    '''
    @classmethod
    def setUp(self):
        self.batch_refresh_period = 0.1
        self.bf = CountdownBloomFilter(1000, 0.02, 5)

    def test_empty(self):
        assert len(self.bf) == 0
        assert self.bf.cellarray.nonzero()[0].shape == (0,)

    def test_cellarray(self):
        assert self.bf.cellarray.shape == (8148,)

    def test_add(self):
        existing = self.bf.add('random_uuid')
        assert existing == False
        existing = self.bf.add('random_uuid')
        assert existing == True
        assert (self.bf.cellarray.nonzero()[0] == np.array([ 228, 2104, 3151, 4372, 6496, 7449])).all()

    def test_single_batch_expiration(self):
        existing = self.bf.add('random_uuid')
        assert existing == False
        existing = self.bf.add('random_uuid')
        assert existing == True
        nzi = self.bf.cellarray.nonzero()[0]
        assert (self.bf.cellarray[nzi] == np.array([255, 255, 255, 255, 255, 255], dtype=np.uint8)).all()
        self.bf.batched_expiration_maintenance(self.batch_refresh_period)
        print self.bf.cellarray[nzi]
        assert (self.bf.cellarray[nzi] == np.array([249, 250, 250, 250, 250, 250], dtype=np.uint8)).all()

    def test_expiration(self):
        existing = self.bf.add('random_uuid')
        assert existing == False
        existing = self.bf.add('random_uuid')
        assert existing == True
        nzi = self.bf.cellarray.nonzero()[0]
        # Check membership just before expiration
        for i in range(49):
            self.bf.batched_expiration_maintenance(self.batch_refresh_period)
        existing = 'random_uuid' in self.bf
        print existing
        assert existing == True
        # Check membership right after expiration
        self.bf.batched_expiration_maintenance(self.batch_refresh_period)
        existing = 'random_uuid' in self.bf
        print existing
        assert existing == False

    
class ScalableCountdownBloomFilter(unittest.TestCase):
    '''
    Tests for CountdownBloomFilter
    '''
    @classmethod
    def setUp(self):
        pass


if __name__ == '__main__':
     unittest.main()