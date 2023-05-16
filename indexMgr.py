import os
import json
from copy import deepcopy

from exception import IndexMgrException

from bPlusTree import Node
from bPlusTree import LeafNode
from bPlusTree import BPlusTree

'''
管理数据库索引，实现 B+ 树数据结构
'''


def bytes2String(values):  # 如果values里面有bytes，则转换为string。其他数据类型不改变
    for i, value in enumerate(values):  # 注意b'字符串和原生字符串的区别
        if isinstance(value, bytes):
            values[i] = values[i].decode('utf-8')

class IndexMgr:
    # 实现的是非聚簇索引
    order = 4  # 2-3-4树

    '''
        self.tables 的格式：
        {'stu': {'uniqueKeyName1': tree1, 'uniqueKeyName2': tree2}, 'tableName': ...}
    '''

    def __init__(self, path):
        self.tables = {}  # 用索引表示的逻辑表
        self.path = os.path.join(path, 'dbfiles/indices')
        self.indexFile = os.path.join(self.path, 'indexFile.json')
        #  在主键上自动创建的索引不会出现在键值对上
        if not os.path.exists(self.path):
            os.makedirs(self.path)
            indexFile = open(self.indexFile, 'w')
            indexFile.close()
            self.save()
        self.load()
    
    def save(self):
        writeTables = {}
        for tableName, trees in self.tables.items():
            writeTables[tableName] = {}
            for primaryKey, tree in trees.items():
                writeTables[tableName][primaryKey] = self.recursivelyStoreNode(tree.root)
        
        with open(self.indexFile, 'w') as indexFile:
            json.dump(writeTables, indexFile)

    def recursivelyStoreNode(self, root: Node):
        if not isinstance(root, Node):  # 到头了
            return root

        rootDict = {
            'keys': root.keys,
            'isLeaf': root.isLeaf()
        }
        rootDict['children'] = []
        for child in root.children:
            #  **重要语句** 嵌套存字典
            subDict = self.recursivelyStoreNode(child)
            rootDict['children'].append(subDict)

        return rootDict

    def load(self):
        with open(self.indexFile, 'r') as indexFile:
            readTables = json.loads(indexFile.read())
            for tableName, trees in readTables.items():
                self.tables[tableName] = {}
                for uniqueKey, tree in trees.items():
                    loadedTree = BPlusTree()
                    self.tables[tableName][uniqueKey] = loadedTree
                    if len(tree['keys']) == 0:  # 空的
                        loadedTree.root = LeafNode()
                    elif tree['isLeaf'] is True:  # 根结点是叶子结点
                        loadedTree.root = LeafNode(
                            order=IndexMgr.order,
                            parent=None,
                            keys=tree['keys'],
                            children=tree['children']
                        )
                    else:  # 非叶子结点
                        loadedTree.root = Node(
                            order=IndexMgr.order,
                            parent=None,
                            keys=tree['keys'],
                            children=tree['children']
                        )
                        loadedTree.root.children = self.recursivelyLoadChilren(
                                                            tree['children'],
                                                            loadedTree.root
                                                        )
                    self.loadLeafs(loadedTree)  # 建立链表

    def recursivelyLoadChilren(self, childrenList, parent):  # 递归向下地构造整棵树
        loadedChildren = []
        for child in childrenList:
            if child['isLeaf'] is True:
                childNode = LeafNode(
                    order=IndexMgr.order,
                    parent=parent,
                    keys=child['keys'],
                    children=child['children']
                )
                loadedChildren.append(childNode)
            else:
                childNode = Node(
                    order=IndexMgr.order,
                    parent=parent,
                    keys=child['keys'],
                    children=child['children']
                )
                loadedChildren.append(childNode)
                childNode.children = self.recursivelyLoadChilren(
                                        child['children'],
                                        loadedChildren[-1]
                                    )
        return loadedChildren

    def loadLeafs(self, loadedTree: BPlusTree):
        # 全部load完成后，还要对叶子结点建立链表
        root = loadedTree.root
        leftmostLeaf = root.getLeftmostLeaf()
        rightmostLeaf = root.getRightmostLeaf()

        curLeaf = leftmostLeaf
        curLeaf.prevLeaf = None
        while curLeaf != rightmostLeaf:
            tmpNode = curLeaf
            nextSibling = None
            while nextSibling is None:
                nextSibling = tmpNode.getNextSibling()
                tmpNode = tmpNode.parent
            nextLeaf = nextSibling.getLeftmostLeaf()
            curLeaf.nextLeaf, nextLeaf.prevLeaf, curLeaf = \
                nextLeaf, curLeaf, nextLeaf
        curLeaf.nextLeaf = None

    def getUniqueKeysWithIndex(self, tableName):  # API调用，给出在哪些uniqueKey上建立了索引
        uniqueKeysWithIndex = []
        trees = self.tables[tableName]
        for uniqueKey in trees.keys():
            uniqueKeysWithIndex.append(uniqueKey)
        return uniqueKeysWithIndex

    def insertRecord(self, tableName, values, insertPos, uniqueKeyHash):
        bytes2String(values)  # 先转换
        trees = self.tables[tableName]
        for uniqueKey, tree in trees.items():
            if uniqueKey in uniqueKeyHash.keys():   # 在这个字段上建立过索引
                uniqueIndex = uniqueKeyHash[uniqueKey]
                value = values[uniqueIndex]
                tree.insert(key=value, value=insertPos)

    def deleteRecord(self, tableName, values, uniqueKeyHash):
        #  删除多条记录，需要API调用多次deleteRecord()
        trees = self.tables[tableName]
        for uniqueKey, tree in trees.items():
            if uniqueKey in uniqueKeyHash.keys():
                uniqueIndex = uniqueKeyHash[uniqueKey]
                value = values[uniqueIndex]
                tree.delete(key=value)

    def createTable(self, tableName, primaryKey):
        newTree = BPlusTree(order=IndexMgr.order)
        self.tables[tableName] = {}
        self.tables[tableName][primaryKey] = newTree

    def dropTable(self, tableName):
        self.tables.pop(tableName)

    def createIndex(self, tableName, uniqueKey, uniqueKeyValuesDict):
        trees = self.tables[tableName]
        newTree = BPlusTree(order=IndexMgr.order)
        trees[uniqueKey] = newTree
        for uniqueKeyValue, uniqueKeyAddr in uniqueKeyValuesDict.items():
            newTree.insert(key=uniqueKeyValue, value=uniqueKeyAddr)

    def dropIndex(self, tableName, keyName):
        self.tables[tableName].pop(keyName)

    def select4UniqueKey(self, tableName, uniqueKeywheres):  # 仅适合uniqueKey！！！
        # API必须保证只传已经建立了索引的uniqueKey的wheres
        # 如果没有已经建立索引的uniqueKey的wheres，则该函数不应该被调用
        # 返回集合。
        totalAddrs = set()
        for where in uniqueKeywheres:
            attributeName = where['lVal']
            rVal = where['rVal']
            operator = where['operator']
            partialAddrs = self.findLeafs(tableName, attributeName, operator, rVal)

            if totalAddrs == set():
                totalAddrs = partialAddrs
            else:
                totalAddrs = totalAddrs & partialAddrs  # 交集

        return totalAddrs

    def findLeafs(self, tableName, uniqueKey, operator, rVal):  # 条件查找。返回所有地址的集合
        tree = self.tables[tableName][uniqueKey]
        root = tree.root
        addrs = set()

        if operator == '<>' or operator == '!=':
            leaf = root.getLeftmostLeaf()
            flag, exceptedLeaf, index = tree.find(rVal)  # 被排除的Leaf
            if flag is False:  # 全要了
                addrs = tree.getAllData()
            else:  # 要有一个排除
                while leaf:
                    for i, addr in enumerate(leaf.children):
                        if leaf == exceptedLeaf and i == index:  # 要排除一个
                            continue
                        addrs.add(addr[0])
                    leaf = leaf.nextLeaf
        elif operator == '=' or operator == '==':
            leaf = root.getLeftmostLeaf()
            flag, includedLeaf, index = tree.find(rVal)  # 只要这个Leaf
            if flag is False:  # 全不要
                addrs = set()
            else:  # 只要一个
                addrs.add(includedLeaf.children[index][0])
        elif operator == '<=':
            leaf = root.getLeftmostLeaf()
            flag, includedLeaf, index = tree.find(rVal)
            if flag is False:  # 找不到
                if index == BPlusTree.INF:  # 全都要
                    addrs = tree.getAllData()
                elif index == -BPlusTree.INF:  # 全不要
                    addrs = set()
                else:
                    flag = False
                    while leaf and not flag:
                        for i, addr in enumerate(leaf.children):
                            if leaf == includedLeaf and i + 1 == index:  # 注意是i + 1
                                flag = True  # 结束
                            addrs.add(addr[0])
                        leaf = leaf.nextLeaf
            else:  # 找得到
                flag = False
                while leaf and not flag:
                    for i, addr in enumerate(leaf.children):
                        if leaf == includedLeaf and i == index:  # 注意是i
                            flag = True  # 结束
                        addrs.add(addr[0])
                    leaf = leaf.nextLeaf
        elif operator == '>=':
            leaf = root.getRightmostLeaf()
            flag, includedLeaf, index = tree.find(rVal)
            if flag is False:  # 找不到
                if index == BPlusTree.INF: 
                    addrs = set()
                elif index == -BPlusTree.INF:
                    addrs = tree.getAllData()
                else:
                    flag = False
                    while leaf and not flag:
                        for i, addr in enumerate(leaf.children):  
                            if leaf == includedLeaf:  
                                flag = True 
                                if i < index:
                                    continue
                            addrs.add(addr[0])
                        leaf = leaf.prevLeaf
            else:  # 找得到
                flag = False
                while leaf and not flag:
                    for i, addr in enumerate(leaf.children):  
                        if leaf == includedLeaf:  
                            flag = True 
                            if i < index:
                                continue
                        addrs.add(addr[0])
                    leaf = leaf.prevLeaf
        elif operator == '<':
            leaf = root.getLeftmostLeaf()
            flag, includedLeaf, index = tree.find(rVal)
            if flag is False:  # 找不到
                if index == BPlusTree.INF:  # 全都要
                    addrs = tree.getAllData()
                elif index == -BPlusTree.INF:  # 全不要
                    addrs = set()
                else:
                    flag = False
                    while leaf and not flag:
                        for i, addr in enumerate(leaf.children):
                            if leaf == includedLeaf and i + 1 == index:  # 注意是i + 1
                                flag = True  # 结束
                            addrs.add(addr[0])
                        leaf = leaf.nextLeaf
            else:  # 找得到
                flag = False
                while leaf and not flag:
                    for i, addr in enumerate(leaf.children):
                        if leaf == includedLeaf and i == index:  # 注意要及时结束
                            flag = True  # 结束
                            break
                        addrs.add(addr[0])
                    leaf = leaf.nextLeaf
        elif operator == '>':
            leaf = root.getRightmostLeaf()
            flag, includedLeaf, index = tree.find(rVal)
            if flag is False:  # 找不到
                if index == BPlusTree.INF: 
                    addrs = set()
                elif index == -BPlusTree.INF:
                    addrs = tree.getAllData()
                else:
                    flag = False
                    while leaf and not flag:
                        for i, addr in enumerate(leaf.children):  
                            if leaf == includedLeaf:  
                                flag = True 
                                if i < index:
                                    continue
                            addrs.add(addr[0])
                        leaf = leaf.prevLeaf
            else:  # 找得到
                flag = False
                while leaf and not flag:
                    for i, addr in enumerate(leaf.children):  
                        if leaf == includedLeaf:  
                            flag = True 
                            if i < index + 1:
                                continue
                        addrs.add(addr[0])
                    leaf = leaf.prevLeaf
        else:
            raise IndexMgrException('where 子句中出现了不支持的运算符！')       

        return addrs

    def checkUnique(self, tableName, insertValues, uniqueKeyWithIndexHash):
        trees = self.tables[tableName]
        for uniqueKey, tree in trees.items():
            uniqueIndex = uniqueKeyWithIndexHash[uniqueKey]
            value = insertValues[uniqueIndex]
            flag, _, __ = tree.find(value)
            if flag is True:  # 找到了
                raise IndexMgrException(f'记录插入失败，原因它违反了表中属性 {uniqueKey} 定义的唯一性')

if __name__ == '__main__':
    tableName = 'test'
    primaryKey = 'id'
    primaryKeyPos = 0
    indexMgr = IndexMgr(os.getcwd())
    indexMgr.dropTable(tableName)
    indexMgr.save()
    exit(0)
    indexMgr.createTable(tableName, primaryKey)
    tree = indexMgr.tables[tableName][primaryKey]

    uniqueKeywheres = [{
        'lVal': 'id',
        'rVal': 7,
        'operator': '>'
    }]
    selectSet = indexMgr.select4UniqueKey(tableName, uniqueKeywheres)

    indexMgr.save()
    exit(0)

    indexMgr.createTable(tableName, primaryKey)
    indexMgr.insertRecord(tableName, [1, 'wzh'], 0, {'id': 0})
    indexMgr.insertRecord(tableName, [2, 'wsa'], 1, {'id': 0})
    indexMgr.insertRecord(tableName, [3, 'fa'], 4, {'id': 0})
    indexMgr.insertRecord(tableName, [4, 'wdsh'], 2, {'id': 0})
    indexMgr.insertRecord(tableName, [5, 'wzh'], 3, {'id': 0})
    indexMgr.insertRecord(tableName, [6, 'wash'], 5, {'id': 0})
    indexMgr.insertRecord(tableName, [11, 'ddh'], 6, {'id': 0})
    indexMgr.insertRecord(tableName, [13, 'dzh'], 7, {'id': 0})
    indexMgr.insertRecord(tableName, [15, 'dzh'], 9, {'id': 0})
    indexMgr.insertRecord(tableName, [16, 'dzh'], 11, {'id': 0})
    indexMgr.save()

    tree = indexMgr.tables[tableName][primaryKey]
    print(tree.getAllData())

