import time

class HashFilter(object):
    '''
    Plain Temporal Hash Filter for testing purposes
    '''
    def __init__(self, expiration):
        self.expiration = expiration
        self.unique_items = {}

    def add(self, key, timestamp = None):
        timestamp = float(timestamp)
        if key in self.unique_items:
            if timestamp < self.unique_items[key]:
                self.unique_items[key] = timestamp + self.expiration
                return True
            else:
                self.unique_items[key] = timestamp + self.expiration
                return False
        else:
            self.unique_items[key] = timestamp + self.expiration
            return False

    def contains(self, key, timestamp):
        timestamp = float(timestamp)
        if key in self.unique_items:
            if timestamp < self.unique_items[key]:
                return True
            else:
                del self.unique_items[key]
                return False
