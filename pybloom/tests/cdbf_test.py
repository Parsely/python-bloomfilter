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
        self.expiration = 5.0
        self.bf = CountdownBloomFilter(1000, 0.02, self.expiration)

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

    def test_touch(self):
        pass

    def test_compute_refresh_time(self):
        assert self.bf.compute_refresh_time() == 2.4132205876674775e-06

    def test_single_batch_expiration(self):
        existing = self.bf.add('random_uuid')
        assert existing == False
        existing = self.bf.add('random_uuid')
        assert existing == True
        nzi = self.bf.cellarray.nonzero()[0]
        assert (self.bf.cellarray[nzi] == np.array([255, 255, 255, 255, 255, 255], dtype=np.uint8)).all()
        self.bf.batched_expiration_maintenance(self.batch_refresh_period)
        assert (self.bf.cellarray[nzi] == np.array([249, 250, 250, 250, 250, 250], dtype=np.uint8)).all()

    def _test_expiration_realtime(self):
        existing = self.bf.add('random_uuid')
        assert existing == False
        existing = self.bf.add('random_uuid')
        assert existing == True
        elapsed = 0
        start = time.time()
        while existing:
            t1 = time.time()
            if elapsed:
                self.bf.batched_expiration_maintenance(elapsed)
            existing = 'random_uuid' in self.bf
            t2 = time.time()
            elapsed = t2 - t1
        experimental_expiration = time.time() - start
        assert (experimental_expiration - self.expiration) < 0.2 # Arbitrary error threshold

    def test_expiration(self):
        existing = self.bf.add('random_uuid')
        assert existing == False
        existing = self.bf.add('random_uuid')
        assert existing == True
        nzi = self.bf.cellarray.nonzero()[0]
        # Check membership just before expiration
        nbr_step = int(self.expiration / self.batch_refresh_period)
        for i in range(nbr_step - 1):
            self.bf.batched_expiration_maintenance(self.batch_refresh_period)
        existing = 'random_uuid' in self.bf
        assert existing == True
        # Check membership right after expiration
        self.bf.batched_expiration_maintenance(self.batch_refresh_period)
        existing = 'random_uuid' in self.bf
        assert existing == False

    def test_count_estimate(self):
        for i in range(500):
            self.bf.add(str(i))
        assert self.bf.count == 500
        self.bf.batched_expiration_maintenance(2.5)
        for i in range(500,1000):
            self.bf.add(str(i))
        assert self.bf.count == 992
        for i in range(26):
            self.bf.batched_expiration_maintenance(0.1)
        assert self.bf.count == 501
        self.assertAlmostEqual(self.bf.estimate_z, 0.309, places=3)
        self.assertAlmostEqual(float(self.bf.cellarray.nonzero()[0].shape[0]) / self.bf.num_bits, 0.309, places=3)



class ScalableCountdownBloomFilterTests(unittest.TestCase):
    '''
    Tests for ScalableCountdownBloomFilter
    '''
    @classmethod
    def setUp(self):
        self.batch_refresh_period = 0.1
        self.expiration = 5.0
        self.bf = ScalableCountdownBloomFilter(initial_capacity=1000, error_rate=0.02, expiration=self.expiration)

    def test_expiration(self):
        pass

    def test_scale_initialization(self):
        for i in range(3000):
            self.bf.add(str(i))
        assert len(self.bf.filters) == 2
        assert self.bf.filters[1].capacity == 2000
        assert self.bf.filters[1].error_rate == 0.016200000000000003

    def test_scale_count(self):
        for i in range(3000):
            self.bf.add(str(i))
        assert self.bf.filters[0].count == 1000
        assert self.bf.filters[1].count == 1951

    def test_scale_pointer(self):
        # This phase will create two sub filter
        for i in range(0,3050):
            self.bf.add(str(i))
        assert self.bf.pointer == 1

        # Here we simulate expired item in the first filter
        self.bf.filters[0].count = 100

        # The new item should be forward to the first available filter
        self.bf.add('an_other_random_uuid')
        assert self.bf.pointer == 0
        for i in range(4000,4992):
            self.bf.add(str(i))
            assert self.bf.pointer == 0

        self.bf.add('an_other_random_uuid2')
        assert self.bf.pointer == 2
        assert self.bf.filters[0].count == 1000
        assert len(self.bf.filters) == 3


    def test_add(self):
        existing = self.bf.add('random_uuid')
        assert existing == False
        existing = self.bf.add('random_uuid')
        assert existing == True
        assert (self.bf.filters[0].cellarray.nonzero()[0] == np.array([1360,1600,3789,4794,6882,8087])).all()

    

if __name__ == '__main__':
     unittest.main()
