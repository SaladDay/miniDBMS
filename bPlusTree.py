import math

class Node:
    def __init__(self, order=4, parent=None, keys=[], children=[]) -> None:
        self.order = order
        self.parent: Node = parent
        self.keys = keys
        self.children = children

    def __str__(self) -> str:
        return 'Node: keys =' + str(self.keys)

    def __eq__(self, other):
        return self.keys == other.keys

    def isEmpty(self):
        return len(self.keys) == 0

    def isFull(self):
        return len(self.keys) == self.order - 1

    def isOverflow(self):
        return len(self.keys) >= self.order

    def isNearlyUnderflow(self):
        return len(self.keys) <= math.ceil(self.order / 2)
    
    def isUnderflow(self):
        return len(self.keys) < math.ceil(self.order / 2) 

    def isRoot(self):
        return self.parent is None

    def isLeaf(self):
        return isinstance(self, LeafNode)
    
    def split(self):  # 将一个满的结点分裂
        leftNode = Node(self.order)
        rightNode = Node(self.order)
        leftNode.parent = self
        rightNode.parent = self

        splitConst = int(self.order // 2)  # 分割常数（mid）

        leftNode.keys = self.keys[ : splitConst]
        leftNode.children = self.children[ : splitConst + 1]

        rightNode.keys = self.keys[splitConst + 1 : ]
        rightNode.children = self.children[splitConst + 1 : ]

        self.children = [leftNode, rightNode]
        self.keys = [self.keys[splitConst]]

        for child in leftNode.children:
            if isinstance(child, Node):
                child.parent = leftNode
        
        for child in rightNode.children:
            if isinstance(child, Node):
                child.parent = rightNode
        
        return self  # 返回分裂后的父亲

    def findNextLevel(self, key):
        for i, existedKey in enumerate(self.keys):
            if key < existedKey:
                return self.children[i], i
            elif i + 1 == len(self.keys):  # 最右边
                return self.children[i + 1], i + 1

    def findLeaf(self, key):
        while not self.isLeaf():
            self, _ = self.findNextLevel(key)
        return self

    def mergeUp(self, child, index):
        # 插入过程中复用的函数。将child合并到self
        self.children.pop(index)
        pivot = child.keys[0]

        for grandChild in child.children:
            if isinstance(grandChild, Node):
                grandChild.parent = self

        for i, existedKey in enumerate(self.keys):
            if pivot < existedKey:
                self.keys = self.keys[ : i] + [pivot] + self.keys[i : ]
                self.children = self.children[ : i] + child.children + self.children[i : ]
                return
            elif i + 1 == len(self.keys):
                self.keys += [pivot]
                self.children += child.children
                return

    def getPrevSibling(self):  # 左边的兄弟
        if self.isRoot() or not self.keys:
            return None
        _, index = self.parent.findNextLevel(self.keys[0])
        if index >= 1:  # 自己不是最左边的
            return self.parent.children[index - 1] 
        else:
            return None

    def getNextSibling(self):
        if self.isRoot() or not self.keys:
            return None
        _, index = self.parent.findNextLevel(self.keys[0])
        if index + 1 < len(self.parent.children):  # 自己不是最右边的
            return self.parent.children[index + 1] 
        else:
            return None

    def borrowLeftNode(self, sibling, parentIndex):
        parentKey = self.parent.keys.pop(-1)
        siblingKey = sibling.keys.pop(-1)
        child = sibling.children.pop(-1)
        child.parent = self

        self.parent.keys.insert(0, siblingKey)
        self.keys.insert(0, parentKey)
        self.children.insert(0, child)

    def borrowRightNode(self, sibling, parentIndex):
        parentKey = self.parent.keys.pop(0)
        siblingKey = sibling.keys.pop(0)
        child = sibling.children.pop(0)
        child.parent = self

        self.parent.keys.append(siblingKey)
        self.keys.append(parentKey)
        self.children.append(child)

    def getLeftmostLeaf(self):
        if not self:
            return None

        while not self.isLeaf():
            self = self.children[0]
        return self

    def getRightmostLeaf(self):
        if not self:
            return None

        while not self.isLeaf():
            self = self.children[-1]
        return self

class LeafNode(Node):
    def __init__(self, order=4, parent=None, keys=[], children=[]) -> None:
        super().__init__(order, parent, keys, children)
        self.prevLeaf = None  # 叶子链表的上一个
        self.nextLeaf = None
        # 特别注意：叶子结点的children是特殊的：它是value

    def __str__(self) -> str:
        return 'LeafNode: keys =' + str(self.keys)
    
    def addKeyAndValue(self, key, value):
        if not self.keys:  # 结点没有key
            self.keys.append(key)  # 直接插入
            self.children.append([value])
            return

        for i, existedKey in enumerate(self.keys):  # 在适当的位置插入
            # print('self.keys = ', self.keys)
            if key == existedKey:
                self.children[i].append(value)
                return
            elif key < existedKey:
                self.keys = self.keys[ : i] + [key] + self.keys[i :]
                self.children = self.children[ : i] + [[value]] + self.children[i : ]
                return
            elif i + 1 == len(self.keys):
                self.keys.append(key)
                self.children.append([value])
                return

    def split(self):  # override
        topNode = Node(self.order)
        siblingNode = LeafNode(self.order)
        splitConst = int(self.order // 2)

        self.parent = topNode
        siblingNode.parent = topNode

        siblingNode.keys = self.keys[splitConst : ]
        siblingNode.children = self.children[splitConst : ]
        siblingNode.prevLeaf = self
        siblingNode.nextLeaf = self.nextLeaf

        topNode.keys = [siblingNode.keys[0]]
        topNode.children = [self, siblingNode]

        self.keys = self.keys[ : splitConst]
        self.children = self.children[ : splitConst]
        self.nextLeaf = siblingNode

        return topNode

    def borrowLeftNode(self, sibling, parentIndex):  # override
        key = sibling.keys.pop(-1)
        data = sibling.children.pop(-1)
        self.keys.insert(0, key)
        self.children.insert(0, data)
        self.parent.keys[parentIndex - 1] = key

    def borrowRightNode(self, sibling, parentIndex):
        key = sibling.keys.pop(0)
        data = sibling.children.pop(0)
        self.keys.append(key)
        self.children.append(data)
        self.parent.keys[parentIndex] = sibling.keys[0]


class BPlusTree:
    INF = 0x3f3f3f3f
    def __init__(self, order=4) -> None:
        self.order = order
        self.root = LeafNode(
            order=order, 
            parent=None,
            keys=[],
            children=[]
        )  # 根结点
        #  非常奇怪的一点：如果写LeafNode(order=order)会错！必须传所有参数

    def find(self, key):
        node = self.root
        if node.isEmpty():  # 空树
            return False, node, -1
        node = node.findLeaf(key)

        if node == self.root.getRightmostLeaf() and node.keys[-1] < key:
            return False, node, BPlusTree.INF  # 太大了
        if node == self.root.getLeftmostLeaf() and node.keys[0] > key:
            return False, node, -BPlusTree.INF  # 太小了

        for i, existedKey in enumerate(node.keys):
            if existedKey == key:
                return True, node, i
            if existedKey > key:
                return False, node, i
            if i + 1 == len(node.keys):
                return False, node, i + 1

    def insert(self, key, value):
        node = self.root
        node = node.findLeaf(key)

        node.addKeyAndValue(key, value)  # node必为LeafNode

        while node.isOverflow():  #  溢出了
            if not node.isRoot():  # 不是根
                parent = node.parent
                node = node.split()  # 分裂并设定为父亲
                _, index = parent.findNextLevel(node.keys[0])
                parent.mergeUp(node, index)
                node = parent  # 迭代向上检查
            else:
                node = node.split()
                self.root = node

    @staticmethod
    def mergeOnDelete(leftNode: Node, rightNode: Node):
        parent = leftNode.parent

        _, index = parent.findNextLevel(leftNode.keys[0])
        parentKey = parent.keys.pop(index)
        parent.children.pop(index)
        parent.children[index] = leftNode
        
        if leftNode.isLeaf() and rightNode.isLeaf():
            leftNode.nextLeaf = rightNode.nextLeaf
        else:
            leftNode.keys.append(parentKey)
            for rightNodeChild in rightNode.children:
                rightNodeChild.parent = leftNode

        leftNode.keys += rightNode.keys
        leftNode.children += rightNode.children

    def delete(self, key):
        node = self.root
        node = node.findLeaf(key)

        if key not in node.keys:
            return False
        
        index = node.keys.index(key)
        node.children[index].pop()  # 删

        if len(node.children[index]) == 0:  # 该结点删完了
            node.children.pop(index)  # 删掉 [] stub
            node.keys.pop(index)

            while node.isUnderflow() and not node.isRoot():
                prevSibling = node.getPrevSibling()
                nextSibling = node.getNextSibling()
                _, parentIndex = node.parent.findNextLevel(key)

                if prevSibling is not None and not prevSibling.isNearlyUnderflow():  # 尽可能借用
                    node.borrowLeftNode(prevSibling, parentIndex)
                elif nextSibling is not None and not nextSibling.isNearlyUnderflow():
                    node.borrowRightNode(nextSibling, parentIndex)
                elif prevSibling is not None and prevSibling.isNearlyUnderflow():  # 借不到再向上递归调整
                    self.mergeOnDelete(prevSibling, node)
                elif nextSibling is not None and nextSibling.isNearlyUnderflow():
                    self.mergeOnDelete(node, nextSibling)

                node = node.parent  # 迭代向上
            
            if node.isRoot() and not node.isLeaf() and len(node.children) == 1:
                # 删除过程中出现了冗余根结点
                self.root = node.children[0]
                self.root.parent = None

        return True

    def getAllData(self):  # 正序遍历B+树叶子链表。返回集合
        data = set()
        node = self.root.getLeftmostLeaf()

        while node is not None:
            for existedData in node.children:
                data.add(existedData[0])
            node = node.nextLeaf
        return data

if __name__ == '__main__':
    tree = BPlusTree(order=4)
    n = 10
    for i in range(n):
        tree.insert(i * 4, i * 4)

    # tp = tree.find(5)
    # print(tp[0], tp[1], tp[2])
    print(tree.getAllData())

    newTree = BPlusTree(order=4)

    print(newTree.getAllData())
    
