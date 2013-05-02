import time

class HashFilter(object):
    '''
    Plain Hash Filter for testing purposes
    '''
    def __init__(self, expiration):
        self.expiration = expiration
        self.unique_items = {}

    def add(self, key):
        if key in self.unique_items:
            timestamp = time.time()
            self.unique_items[key] = timestamp + self.expiration
            return True
        else:
            timestamp = time.time()
            self.unique_items[key] = timestamp + self.expiration
            return False

    def __contains__(self, key):
        timestamp = time.time()
        if key in self.unique_items:
            if timestamp < self.unique_items[key]:
                return True
            else:
                del self.unique_items[key]
                return False

