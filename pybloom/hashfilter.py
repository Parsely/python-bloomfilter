import time

class HashFilter(object):
    '''
    Plain Temporal Hash Filter for testing purposes
    '''
    def __init__(self, expiration):
        self.expiration = expiration
        self.unique_items = {}

    def add(self, key, timestamp = None):
        if key in self.unique_items:
            if not timestamp:
                timestamp = time.time()
            self.unique_items[key] = int(timestamp) + self.expiration
            return True
        else:
            if not timestamp:
                timestamp = time.time()
            self.unique_items[key] = int(timestamp) + self.expiration
            return False

    def contains(self, key, timestamp):
        timestamp = int(timestamp)
        if key in self.unique_items:
            if timestamp < self.unique_items[key]:
                return True
            else:
                del self.unique_items[key]
                return False

    def __contains__(self, key):
        timestamp = time.time()
        if key in self.unique_items:
            if timestamp < self.unique_items[key]:
                return True
            else:
                del self.unique_items[key]
                return False

