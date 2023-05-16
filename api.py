import time
from catalogMgr import CatalogMgr
from indexMgr import IndexMgr
from bufferMgr import BufferMgr

from exception import QueryException

class ResultPrinter:
    def printSelect(attributes, attributeIndices, records):
        columnFrameWidth = 17  # 字段之间的宽度
        columnWidth = 14  # 单字段的最大宽度
        print('-' * (columnFrameWidth * len(attributeIndices) + 1))
        for attribute in attributes:
            outputStr = str(attribute)
            if len(outputStr) > columnWidth:
                outputStr = outputStr[0:columnWidth]
            print('|', outputStr.center(columnWidth + 1), end='')
        print('|')

        print('-' * (columnFrameWidth * len(attributeIndices) + 1))
        for record in records:
            for index in attributeIndices:
                outputStr = str(record[index])
                if len(outputStr) > columnWidth:
                    outputStr = outputStr[0:columnWidth]
                print('|', outputStr.center(columnWidth + 1), end='')
            print('|')
        print('-' * (columnFrameWidth * len(attributeIndices) + 1))

        print(f'查询到 {len(records)} 条记录。', end='')
        

class Api:
    def __init__(self, path):
        self.catalogMgr = CatalogMgr(path)
        self.indexMgr = IndexMgr(path)
        self.bufferMgr = BufferMgr(path, self.catalogMgr.tables)

    def save(self):
        self.catalogMgr.save()
        self.indexMgr.save()
        self.bufferMgr.save()

    def createTable(self, tableName, attributes, primaryKey):
        startTime = time.time()

        if self.catalogMgr.checkExistTable(tableName):
            raise QueryException(f'表 {tableName} 已存在！')

        self.catalogMgr.createTable(tableName, attributes, primaryKey)
        self.indexMgr.createTable(tableName, primaryKey)
        columns = self.catalogMgr.tables[tableName].columns
        self.bufferMgr.createTable(tableName, columns)

        endTime = time.time()
        print(f'创建表 {tableName} 用了 {(endTime - startTime) * 1000} 毫秒。')

    def createIndex(self, indexName, tableName, attributeName):
        startTime = time.time()

        if not self.catalogMgr.checkExistTable(tableName):
            raise QueryException(f'当前表 {tableName} 不存在！')

        if self.catalogMgr.checkExistIndex(indexName):
            raise QueryException(f'索引 {indexName} 已存在！')

        uniqueKeyWithIndex = self.indexMgr.getUniqueKeysWithIndex(tableName)
        if attributeName in uniqueKeyWithIndex:
            raise QueryException(f'在属性 {attributeName} 上的索引已存在！')

        self.catalogMgr.createIndex(indexName, tableName, attributeName)

        columnHash = self.catalogMgr.getColumnHash(tableName)  # 从columnName到index的映射
        #  全表扫描
        recordsFound, correspondingAddrs = self.bufferMgr.findRecords(
            tableName=tableName, 
            columnHash=columnHash,
            notUniqueKeyWheres=[],
            uniqueKeyWheres=[],
            uniqueKeyResultAddrs=set()
        )
        uniqueKeyIndex = self.catalogMgr.getKeyIndex(tableName, attributeName)
        uniqueKeyValues = []
        for record in recordsFound:
            uniqueKeyValues.append(record[uniqueKeyIndex])

        uniqueKeyValuesDict = dict(zip(uniqueKeyValues, correspondingAddrs))

        self.indexMgr.createIndex(tableName, attributeName, uniqueKeyValuesDict)

        endTime = time.time()
        print(f'创建索引 {indexName} 用了 {(endTime - startTime) * 1000} 毫秒。')        

    def dropTable(self, tableName):
        startTime = time.time()

        if not self.catalogMgr.checkExistTable(tableName):
            raise QueryException(f'当前表 {tableName} 不存在！')

        self.catalogMgr.dropTable(tableName)
        self.indexMgr.dropTable(tableName)
        self.bufferMgr.dropTable(tableName)

        endTime = time.time()
        print(f'删除表 {tableName} 用了 {(endTime - startTime) * 1000} 毫秒。')

    def dropIndex(self, indexName):
        startTime = time.time()

        if not self.catalogMgr.checkExistIndex(indexName):
            raise QueryException(f'当前索引 {indexName} 不存在！')

        tableName, keyName = self.catalogMgr.dropIndex(indexName)
        self.indexMgr.dropIndex(tableName, keyName)

        endTime = time.time()
        print(f'删除在表 {tableName} 的属性 {keyName} 上的索引 {indexName} 用了 {(endTime - startTime) * 1000} 毫秒。')

    def select(self, tableName, attributes, wheres):
        startTime = time.time()

        if not self.catalogMgr.checkExistTable(tableName):
            raise QueryException(f'当前表 {tableName} 不存在！')

        flag, columnName = self.catalogMgr.checkSelectStatement(tableName, attributes, wheres)
        if flag is False:
            raise QueryException(f"表 {tableName} 中不存在名为 {columnName} 的字段")

        uniqueKeyWithIndex = self.indexMgr.getUniqueKeysWithIndex(tableName)
        uniqueKeyWheres = []  # B+树检索
        notUniqueKeyWheres = []  # 全表扫描
        for where in wheres:
            if where['lVal'] in uniqueKeyWithIndex:
                uniqueKeyWheres.append(where)
            else:
                notUniqueKeyWheres.append(where)

        uniqueKeyResultAddrs = self.indexMgr.select4UniqueKey(
            tableName,
            uniqueKeyWheres
        )

        columnHash = self.catalogMgr.getColumnHash(tableName)  # 从columnName到index的映射
        recordsFound, _ = self.bufferMgr.findRecords(
            tableName, 
            columnHash,
            notUniqueKeyWheres,
            uniqueKeyWheres,
            uniqueKeyResultAddrs
        )

        attributeIndices = []
        if attributes == ['*']:
            attributes = list(columnHash.keys())
            attributeIndices = list(columnHash.values())
        else:
            for attribute in attributes:
                attributeIndices.append(columnHash[attribute])

        ResultPrinter.printSelect(attributes, attributeIndices, recordsFound)
        endTime = time.time()
        print(f'查询操作用了 {(endTime - startTime) * 1000} 毫秒。')
    
    def insert(self, tableName, values):
        startTime = time.time()

        if not self.catalogMgr.checkExistTable(tableName):
            raise QueryException(f'当前表 {tableName} 不存在！')
        flag, msg, data = self.catalogMgr.checkValidType(tableName, values)
        if flag is False:
            if msg == 'TooFewColumns':
                raise QueryException(f'表 {tableName} 含有 {data} 列，与插入元素数目不匹配') 
            elif msg == 'CharLenExceeded':
                raise QueryException(f'对表 {tableName} 试图插入的 char 类型数据超出定义的最大长度 {data}')
            else:
                raise QueryException(f'插入表 {tableName} 的过程中出现未知错误')

        uniqueKeyWithIndex = self.indexMgr.getUniqueKeysWithIndex(tableName)
        uniqueKeyHash = self.catalogMgr.getUniqueKeyHash(tableName)
        uniqueKeyWithIndexHash = {}
        uniqueKeyNotWithIndexColumns = []
        for uniqueKey, uniqueKeyIndex in uniqueKeyHash.items():
            if uniqueKey in uniqueKeyWithIndex:
                uniqueKeyWithIndexHash[uniqueKey] = uniqueKeyIndex

        uniqueKeyColumns = self.catalogMgr.getUniqueKeyColumns(tableName)
        for uniqueKeyColumn in uniqueKeyColumns:
            if uniqueKeyColumn.columnName not in uniqueKeyWithIndex:
                uniqueKeyNotWithIndexColumns.append(uniqueKeyColumn)

        # B+树检查唯一性
        self.indexMgr.checkUnique(tableName, values, uniqueKeyWithIndexHash)
        # 全表扫描检查唯一性放在bufferMgr.insertRecord里面
        columns = self.catalogMgr.tables[tableName].columns
        insertPos = self.bufferMgr.insertRecord(tableName, values, columns, uniqueKeyNotWithIndexColumns)
        
        self.indexMgr.insertRecord(tableName, values, insertPos, uniqueKeyHash)

        endTime = time.time()
        print(f'插入记录操作用了 {(endTime - startTime) * 1000} 毫秒。')

    def delete(self, tableName, wheres):
        startTime = time.time()

        if not self.catalogMgr.checkExistTable(tableName):
            raise QueryException(f'当前表 {tableName} 不存在！')

        flag, columnName = self.catalogMgr.checkSelectStatement(tableName, ['*'], wheres)
        if flag is False:
            raise QueryException(f"表 {tableName} 中不存在名为 {columnName} 的字段")

        uniqueKeyWithIndex = self.indexMgr.getUniqueKeysWithIndex(tableName)
        uniqueKeyWheres = []  # B+树检索
        notUniqueKeyWheres = []  # 全表扫描
        for where in wheres:
            if where['lVal'] in uniqueKeyWithIndex:
                uniqueKeyWheres.append(where)
            else:
                notUniqueKeyWheres.append(where)

        uniqueKeyResultAddrs = self.indexMgr.select4UniqueKey(
            tableName,
            uniqueKeyWheres
        )

        columnHash = self.catalogMgr.getColumnHash(tableName)  # 从columnName到index的映射
        recordsFound, correspondingAddrs = self.bufferMgr.findRecords(
            tableName, 
            columnHash,
            notUniqueKeyWheres,
            uniqueKeyWheres,
            uniqueKeyResultAddrs
        )

        self.bufferMgr.deleteRecords(tableName, correspondingAddrs)

        uniqueKeyHash = self.catalogMgr.getUniqueKeyHash(tableName)
        for record in recordsFound:
            self.indexMgr.deleteRecord(tableName, record, uniqueKeyHash)
                    
        endTime = time.time()
        print(f'删除 {len(correspondingAddrs)} 条记录用了 {(endTime - startTime) * 1000} 毫秒。')

    def show(self, tableName):
        if tableName is None:  # show tables
            self.catalogMgr.showTables()
        else:
            if not self.catalogMgr.checkExistTable(tableName):
                raise QueryException(f'当前表 {tableName} 不存在！')
            self.catalogMgr.showTable(tableName)

