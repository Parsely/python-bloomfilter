import time

from collections import deque
from pybloom import ScalableBloomFilter

VALID_RES = {'Min': 60,
             'Hour': 3600,
             'Day': 86400}

class DecayScalableBloomFilter(ScalableBloomFilter):
    '''
    Stepwise decaying Bloom Filter
    '''
    def __init__(self, initial_capacity=1000, error_rate=0.01, window_period = '10_Min'):
        super(DecayScalableBloomFilter, self).__init__(initial_capacity, error_rate)
        self.window_period = 60
        self.timestamp = time.time()
        self._setup_window_period(window_period)
        self._expired = False
        self._locked = False

    @property
    def expired(self):
        if time.time() - self.timestamp > self.expiration:
            self._expired = True
        return self._expired

    @property
    def locked(self):
        if time.time() - self.timestamp > self.window_period:
            self._locked = True
        return self._locked

    def _setup_window_period(self, window_period):
        try:
            self.amount, self.res = window_period.split('_')
            self.amount = int(self.amount)
        except ValueError:
            raise Exception('Invalid window period')
        self.window_period = VALID_RES[self.res]
        self.expiration = self.amount * VALID_RES[self.res]
        self.filters = deque(maxlen = self.amount)

class SlidingWindowScalableBloomFilter(object):
    '''
    Sliding Window Bloom Filter using a coarse expiration
    '''

    def __init__(self, initial_capacity=1000, window_period = "10_Min"):
        self.initial_capacity = initial_capacity
        self.error_rate = 0.01
        self._setup_window_period(window_period)

    def _setup_window_period(self, window_period):
        try:
            self.amount, self.res = window_period.split('_')
            self.amount = int(self.amount)
        except ValueError:
            raise Exception('Invalid window period')
        self.window_period = self.amount * VALID_RES[self.res]
        self._reset_filters()

    def _reset_filters(self):
        self.filters = deque(maxlen = self.amount)

    def total_error(self):
        '''
        Return the total error: temporal error + native bf error
        '''
        temporal_error = float(1.0 / self.amount)
        total_error = self.error_rate + temporal_error
        return total_error

    def __contains__(self, key):
        for f in reversed(self.filters):
            if key in f:
                return True
        return False

    def check_expiration(self):
        filter = self.filters[0]
        if filter.expired:
            filter = DecayScalableBloomFilter(initial_capacity=self.initial_capacity,
                                              error_rate=self.error_rate,
                                              window_period="%s_%s" % (str(self.amount),self.res))
            self.filters.append(filter)

    def add(self, key):
        if key in self:
            return True
        if not self.filters:
            filter = DecayScalableBloomFilter(initial_capacity=self.initial_capacity,
                                              error_rate=self.error_rate,
                                              window_period="%s_%s" % (str(self.amount),self.res))
            self.filters.append(filter)
        else:
            filter = self.filters[-1]
            if filters.locked:
                if filters.expired:
                    self._reset_filters()
                filter = DecayScalableBloomFilter(initial_capacity=self.initial_capacity,
                                              error_rate=self.error_rate,
                                              window_period="%s_%s" % (str(self.amount),self.res))
                self.filters.append(filter)

        filter.add(key)
        return False

