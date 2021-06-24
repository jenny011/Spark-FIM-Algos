'''tree'''
import header as myHeader
from time import time

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


#----------------------------------- Below are FPTree codes -------------------------------
#----------------------------------- Below are FPTree codes -------------------------------
#----------------------------------- Below are FPTree codes -------------------------------

class FPTree(Tree):
    def __init__(self):
        super().__init__()
        self.headerTable = myHeader.HeaderTable()
        self.last_in_route = {}

    #-------------------------- nonpublic mutators --------------------------
    def _update(self, node, count):
        node._count += count
        return node._count

    def insert_loop(self, ptr, line, count):
        for item in line:
            if item in self.children(ptr):
                self._update(ptr._children[item], count)
                ptr = ptr._children[item]
            else:
                self._size += 1
                newNode = TreeNode(item, ptr)
                self._update(newNode, count)
                prevHeader = self.get_last(item)
                prevHeader._next = newNode
                ptr._children[item] = newNode
                ptr = newNode

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
            prevHeader = self.get_last(key)
            prevHeader._next = newNode
            self.update_last(key, newNode)
            ptr._children[key] = newNode
            ptr = newNode
            self.insert(ptr, line[1:], count)

    #---------------------------- public methods ------------------------------
    def createHeaderTable(self,dbItems, minsup):
        unsorted = {}
        for key, count in dbItems.items():
            if count >= minsup:
                unsorted[key] = count
        temp = sorted(unsorted.items(), key=lambda x: x[1], reverse=True)
        for item in temp:
            header = self.headerTable.insert(item[0])
            self.last_in_route[item[0]] = header

    # get the last node in the linked list from the headerTable
    def get_last(self, key):
        return self.last_in_route[key]

    def update_last(self, key, header):
        self.last_in_route[key] = header

    # Add one transaction or conditional pattern base
    def add(self, line, count):
        # if record:
        #     f = open('fpGrowthExp/retail50_01/insert.txt', 'a')
        #     start = time()
        sortedLine = []
        for key in self.headerTable.keys():
            if key in line:
                sortedLine.append(key)
        self.insert(self._root, sortedLine, count)
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
        return (path, count)

    def upward_branch_traversal(self, node):
        trx = []
        count = node._count
        while node and node != self._root:
            if node._count > 0:
                trx.append(node._key)
                node._count -= count
            node = node._parent
        if not trx:
            return []
        trx.reverse()
        return (trx, count)

    def __repr__(self):
        r = ''
        for i in self:
            if i[0] == "40":
                print(i[1])
            r += str(i) + ' '
        return r

    def iter_ll(self, key):
        ptr = self.headerTable.find_first(key)
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
