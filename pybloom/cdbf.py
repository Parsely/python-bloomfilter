import math
import hashlib
import numpy as np

from math import floor
from scipy.sparse import lil_matrix
from struct import unpack, pack, calcsize
from pybloom import BloomFilter, ScalableBloomFilter, make_hashfuncs

class CountdownBloomFilter(object):
    '''
    Implementation of a Modified Countdown Bloom Filter. Uses a batched maintenance process instead of a continuous one.

    Sanjuas-Cuxart, Josep, et al. "A lightweight algorithm for traffic filtering over sliding windows."
    Communications (ICC), 2012 IEEE International Conference on. IEEE, 2012.

    http://www-mobile.ecs.soton.ac.uk/home/conference/ICC2012/symposia/papers/a_lightweight_algorithm_for_traffic_filtering_over_sliding__.pdf
    '''
    def __init__(self, capacity, error_rate=0.001, expiration=60):
        self.expiration = expiration
        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")
        if not capacity > 0:
            raise ValueError("Capacity must be > 0")
        num_slices = int(math.ceil(math.log(1.0 / error_rate, 2)))
        bits_per_slice = int(math.ceil(
            (capacity * abs(math.log(error_rate))) /
            (num_slices * (math.log(2) ** 2))))
        self._setup(error_rate, num_slices, bits_per_slice, capacity, 0)
        self.cellarray = np.zeros(self.num_bits).astype(np.uint8)
        self.counter_init = 255
        self.refresh_head = 0
         # This is the unset ratio ... and we keep constant @ 0.5
        self.z = 0.5

    def _setup(self, error_rate, num_slices, bits_per_slice, capacity, count):
        self.error_rate = error_rate
        self.num_slices = num_slices
        self.bits_per_slice = bits_per_slice
        self.capacity = capacity
        self.num_bits = num_slices * bits_per_slice
        self.count = count
        self.make_hashes = make_hashfuncs(self.num_slices, self.bits_per_slice)

    def expiration_maintenance(self):
        '''
        Decrement cell value if not zero
        This maintenance process need to executed each self.compute_refresh_time()
        '''
        if self.cellarray[self.refresh_head] != 0:
            self.cellarray[self.refresh_head] -= 1
        self.refresh_head += 1

    def batched_expiration_maintenance_dev(self, num_iterations):
        '''
        Batched version of expiration_maintenance()
        '''
        if self.z != 0:
            for i in range(num_iterations):
                self.expiration_maintenance()

    def batched_expiration_maintenance(self, num_iterations):
        '''
        Batched version of expiration_maintenance()
        Cython version
        '''
        pass

    def compute_refresh_time(self):
        '''
        Compute the refresh period for the given expiration delay
        '''
        s = float(self.expiration) * (1.0/(self.num_bits)) * (1.0/(self.counter_init - 1 + (1.0/(self.z * (self.num_slices + 1)))))
        return s

    def num_batched_maintenance(self, delay):
        return int(floor(delay / self.compute_refresh_time()))

    def __contains__(self, key):
        if not isinstance(key, list):
            hashes = self.make_hashes(key)
        else:
            hashes = key
        offset = 0
        for k in hashes:
            if self.cellarray[offset + k] == 0:
                return False
            offset += self.bits_per_slice
        return True

    def __len__(self):
        """Return the number of keys stored by this bloom filter."""
        return self.count

    def add(self, key, skip_check=False):
        hashes = self.make_hashes(key)
        if not skip_check and hashes in self:
            return True
        if self.count > self.capacity:
            raise IndexError("BloomFilter is at capacity")
        offset = 0
        for k in hashes:
            self.cellarray[offset + k] = self.counter_init
            offset += self.bits_per_slice
        self.count += 1
        return False


class ScalableCountdownBloomFilter(object):
    SMALL_SET_GROWTH = 2
    LARGE_SET_GROWTH = 4
    FILE_FMT = '<idQd'

    def __init__(self, initial_capacity=100,
                       error_rate=0.001,
                       mode=SMALL_SET_GROWTH,
                       expiration = 60):
        if not error_rate or error_rate < 0:
            raise ValueError("Error_Rate must be a decimal less than 0.")
        self._setup(mode, 0.9, initial_capacity, error_rate)
        self.filters = []
        self.expiration = expiration

    def _setup(self, mode, ratio, initial_capacity, error_rate):
        self.scale = mode
        self.ratio = ratio
        self.initial_capacity = initial_capacity
        self.error_rate = error_rate

    def __contains__(self, key):
        for f in reversed(self.filters):
            if key in f:
                return True
        return False

    def add(self, key):
        if key in self:
            return True
        if not self.filters:
            filter = CountdownBloomFilter(capacity=self.initial_capacity,
                                          error_rate=self.error_rate * (1.0 - self.ratio),
                                          expiration=self.expiration)
            self.filters.append(filter)
        else:
            filter = self.filters[-1]
            if filter.count >= filter.capacity:
                filter = CountdownBloomFilter(capacity=filter.capacity * self.scale,
                                              error_rate=filter.error_rate * self.ratio,
                                              expiration=self.expiration)
                self.filters.append(filter)
        filter.add(key, skip_check=True)
        return False

    @property
    def capacity(self):
        """Returns the total capacity for all filters in this SBF"""
        return sum([f.capacity for f in self.filters])

    @property
    def count(self):
        return len(self)

    def __len__(self):
        """Returns the total number of elements stored in this SBF"""
        return sum([f.count for f in self.filters])
