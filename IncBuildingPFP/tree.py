from time import time
import sys

old_limit = sys.getrecursionlimit()

'''tree'''
#-------------------------- Tree Base -------------------
#-------------------------- Tree Base -------------------
#-------------------------- Tree Base -------------------
class TreeNode():
        def __init__(self, key = None, parent = None, count = 0, link = None):
            self._key = key
            self._parent = parent
            self._count = count
            self._children = {}
            self._next = link

class Tree():
    def __init__(self):
        self._root = TreeNode()
        self._size = 0

    #-------------------------- public accessors -------------------
    def size(self):
        return self._size

    def is_empty(self):
        return self.size() == 0

    #iterators
    def __iter__(self):
        for node in self.preorder():
            yield (node._key, node._count)

    def nodes(self):
        for node in self.preorder():
            yield node

    def keys(self):
        for node in self.preorder():
            yield node._key

    def counts(self):
        for node in self.preorder():
            yield node._count

    def children(self, node):
        for child in node._children.keys():
            yield child

    def preorder(self):
        if not self.is_empty():
            for node in self._subtree_preorder(self._root):
                yield node

    def _subtree_preorder(self, node):
        yield node
        for c in node._children.values():
            for other in self._subtree_preorder(c):
                yield other


#----------------------------------- Below are CanTree codes -------------------------------
#----------------------------------- Below are CanTree codes -------------------------------
#----------------------------------- Below are CanTree codes -------------------------------

class CanTree(Tree):
    def __init__(self):
        super().__init__()
        self.headerTable = {}
        self.last_in_route = {}

    #-------------------------- nonpublic mutators --------------------------
    def _update(self, node, count):
        node._count += count
        return node._count

    # insert with a loop
    def insert_loop(self, ptr, line, count):
        for item in line:
            if item in self.children(ptr):
                self._update(ptr._children[item], count)
                ptr = ptr._children[item]
            else:
                self._size += 1
                newNode = TreeNode(item, ptr)
                self._update(newNode, count)
                prevHeader = self.find_last(item)
                prevHeader._next = newNode
                ptr._children[item] = newNode
                ptr = newNode

    # insert with recursion
    def insert(self, ptr, line, count):
        if not line:
            return
        key = line[0]
        if key in self.children(ptr):
            self._update(ptr._children[key], count)
            ptr = ptr._children[key]
            self.insert(ptr, line[1:], count)
        else:
            self._size += 1
            newNode = TreeNode(key, ptr)
            self._update(newNode, count)
            prevHeader = self.find_last(key)
            prevHeader._next = newNode
            self.update_last(key, newNode)
            ptr._children[key] = newNode
            ptr = newNode
            self.insert(ptr, line[1:], count)

    #---------------------------- public methods ------------------------------
    # create a header table with a canonical order
    def createHeaderTable(self, dbItems):
        temp = sorted(dbItems.items(), key=lambda x: x[0])
        for item in temp:
            self.headerTable[item[0]] = TreeNode()
            self.last_in_route[item[0]] = self.headerTable[item[0]]

    # find the first node in the linked list from the headerTable
    def find_first(self, key):
        return self.headerTable[key]

    # find the last node in the linked list from the headerTable
    def find_last(self, key):
        return self.last_in_route[key]

    def update_last(self, key, node):
        self.last_in_route[key] = node

    # Add one transaction or conditional pattern base
    def add(self, line, count):
        # if record:
        #     f = open('insert.txt', 'a')
        #     start = time()
        if len(line) >= sys.getrecursionlimit():
            sys.setrecursionlimit(len(line) + 10)
            self.insert(self._root, sorted(line), count)
            sys.setrecursionlimit(old_limit)
        else:
            self.insert(self._root, sorted(line), count)
        # if record:
        #     end = time()
        #     f.write(str(end - start) + '\n')
        #     f.close()

    # Get the prefix path which can be readily used added to the conditional pattern base
    def prefix_path(self, node):
        path = []
        count = node._count
        node = node._parent
        while node and node != self._root:
            path.append(node._key)
            node = node._parent
        if not path:
            return None
        path.reverse()
        return (count, path)

    def __repr__(self):
        r = ''
        for i in self:
            r += str(i) + ' '
        return r

    def iter_ll(self, key):
        ptr = self.find_first(key)
        ptr = ptr._next
        while ptr:
            yield ptr
            ptr = ptr._next

    def repr_ll(self, key):
        r = ''
        for node in self.iter_ll(key):
            r += '(' + node._key + ',' + str(node._count) + ') '
        # r = 0
        # for node in self.iter_ll(key):
        #     r += node._count
        return r
