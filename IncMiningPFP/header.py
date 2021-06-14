'''header table'''
class Header():
    __slots__ = '_key', '_next'

    def __init__(self, key, link = None):
      self._key = key
      # self._count = count
      self._next = link

    # def __gt__(self, other):
    #   return self._count > other._count 

    def __repr__(self):
      # return '({0}: {1})'.format(self._key, self._count)
      return str(self._key)

class HeaderTable():
    def __init__(self):
        self._table = []

    def __len__(self):
        return len(self._table)

    # Iterators
    def __iter__(self):
        for header in self._table:  
            yield header._key

    def headers(self):
        for header in self._table:
            yield header

    def keys(self):
        for header in self.headers():  
            yield header._key

    # def counts(self):
    #     for header in self.headers():  
    #         yield header._count

    def insert(self, key):
        header = Header(key)
        self._table.append(header)
        return header

    # Find the header with the key as the first node in the linked list
    def find_first(self, key):
        for header in self.headers():
            if header._key == key:
                return header

    def __repr__(self):
        r = ''
        for i in self:
            r += str(i) + ' '
        return r
