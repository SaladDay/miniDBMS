from exception import QueryException
import os
import json


class CatalogMgr:
    class CatalogTable:  # 逻辑表
        def __init__(self, tableName, columns, primaryKey=None):
            self.tableName = tableName
            self.columns = columns
            self.primaryKey = primaryKey

    class CatalogColumn:  # 逻辑列（字段）
        def __init__(self, columnName, isUnique, type, charLen):
            self.columnName = columnName
            self.isUnique = isUnique
            self.type = type
            self.charLen = charLen
        def __str__(self) -> str:
            return 'CatalogColumn: ' + str(self.columnName)

    '''
    self.tables 的格式：
    {'stu': <CatalogMgr.CatalogTable object>, 'tableName': ...}
    self.indices 的格式：
    {'indexName': {'tableName': ..., 'columnName': ...}, 'indexName2': ...}
    '''

    def __init__(self, path):
        self.tables = {}  # 由表名到逻辑表的哈希
        self.indices = {}  # 由索引名到逻辑索引的哈希
        self.path = os.path.join(path, 'dbfiles/catalogs')
        self.tableCatalog = os.path.join(self.path, 'tableCatalog.json')
        self.indexCatalog = os.path.join(self.path, 'indexCatalog.json')
        if not os.path.exists(self.path):
            os.makedirs(self.path)
            tableCatalog = open(self.tableCatalog, 'w')
            indexCatalog = open(self.indexCatalog, 'w')
            tableCatalog.close()
            indexCatalog.close()
            self.save()
        self.load()

    def save(self):  # 向文件写入逻辑表、逻辑索引
        writeTables = {}
        for tableName, catalogTable in self.tables.items():
            columns = {}  # 由列名到逻辑列的哈希
            for column in catalogTable.columns:
                columns[column.columnName] = {
                    'isUnique': column.isUnique,
                    'type': column.type,
                    'charLen': column.charLen
                }
            writeTables[tableName] = {
                'columns': columns,
                'primaryKey': catalogTable.primaryKey
            }  # 字典嵌套

        with open(self.tableCatalog, 'w') as tableCatalog:
            json.dump(writeTables, tableCatalog)
        with open(self.indexCatalog, 'w') as indexCatalog:
            json.dump(self.indices, indexCatalog)

    def load(self):  # 内存读取逻辑表、逻辑索引
        with open(self.tableCatalog, 'r') as tableCatalog:
            # json.loads()表示读取为Python字典格式
            readTables = json.loads(tableCatalog.read())
            for tableName, columnsAndPk in readTables.items():
                primaryKey = columnsAndPk['primaryKey']
                readColumns = columnsAndPk['columns']
                columns = []
                for columnName, columnOthers in readColumns.items():
                    readColumn = self.CatalogColumn(
                        columnName=columnName, 
                        isUnique=columnOthers['isUnique'], 
                        type=columnOthers['type'], 
                        charLen=columnOthers['charLen']
                    )
                    columns.append(readColumn)
                self.tables[tableName] = self.CatalogTable(
                    tableName, columns, primaryKey)
        with open(self.indexCatalog, 'r') as indexCatalog:
            readIndices = json.loads(indexCatalog.read())
            for indexName, index in readIndices.items():
                self.indices[indexName] = index

    def checkExistTable(self, tableName):
        for curExistTableName in self.tables.keys():
            if tableName == curExistTableName:
                return True
        return False

    def checkExistIndex(self, indexName):
        for curExistIndexName in self.indices.keys():
            if indexName == curExistIndexName:
                return True
        return False

    def createTable(self, tableName, attributes, primaryKey):
        columns = []
        '''
        attributes = {
                'name': name,
                'type': type,
                'charLen': charLen,
                'isUnique': isUnique
            })
        '''
        for attribute in attributes:
            columns.append(self.CatalogColumn(
                attribute['name'],
                attribute['isUnique'],
                attribute['type'],
                attribute['charLen'])
            )

        table = self.CatalogTable(tableName, columns, primaryKey)

        self.tables[tableName] = table

    def createIndex(self, indexName, tableName, attributeName):
        columns = self.tables[tableName].columns
        for column in columns:
            if column.columnName == attributeName and column.isUnique is False:
                raise QueryException(f'抱歉，只支持在声明为 Unique 的属性上创建索引！')
        self.indices[indexName] = {
            'tableName': tableName,
            'columnName': attributeName
        }

    def dropTable(self, tableName):
        self.tables.pop(tableName)
        dropIndexNames = []
        for indexName, indexMsg in self.indices.items():
            if indexMsg['tableName'] == tableName:
                dropIndexNames.append(indexName)
        for dropIndexName in dropIndexNames:
            self.indices.pop(dropIndexName)


    def dropIndex(self, indexName):
        tableName = self.indices[indexName]['tableName']
        keyName = self.indices[indexName]['columnName']
        self.indices.pop(indexName)
        return tableName, keyName  # 给indexMgr用

    def checkValidType(self, tableName, values):  
        # 检查试图插入的记录的各属性类型与表的定义是否匹配
        # 如果试图插入小数，但字段定义是整数，要自动给他转换（截断）
        table = self.tables[tableName]
        if len(table.columns) != len(values):
            return False, 'TooFewColumns', len(table.columns)

        for i, column in enumerate(table.columns):
            if column.type == 'float':
                value = float(values[i])
            elif column.type == 'int':
                value = int(values[i])
            elif column.type == 'char':
                value = values[i]
                if len(value) > column.charLen:
                    return False, 'CharLenExceeded', column.charLen
            else:
                raise QueryException(f'检查数据类型时发生未知错误')
        return True, 'Succeed', None

    def checkSelectStatement(self, tableName, attributes, wheres):
        columnNames = []
        for column in self.tables[tableName].columns:
            columnNames.append(column.columnName)
        if wheres != {}:  # 检查 where
            for where in wheres:
                if where['lVal'] not in columnNames:
                    return False, where['lVal']
        if attributes != ['*']:  # 检查 select ...
            for attribute in attributes:
                if attribute not in columnNames:
                    return False, attribute
        return True, None

    def showTable(self, tableName):
        table = self.tables[tableName]
        for column in table.columns:
            print(
                f'column: {column.columnName}\t\t',
                f'isUnique: {column.isUnique}\t\t',
                f'type: {column.type}\t\t',
                end=''
            )
            if column.type == 'char':
                print(f'charLen: {column.charLen}', end='')
            print('\n', end='')

    def getTableNameList(self):
        return [tableName for tableName in self.tables.keys()]

    def showTables(self):
        for tableName in self.getTableNameList():
            print(tableName)

    def getColumnHash(self, tableName):
        columnHash = {}
        columns = self.tables[tableName].columns
        for i, column in enumerate(columns):
            columnHash[column.columnName] = i
        return columnHash

    def getUniqueKeyHash(self, tableName):
        #  {columnName: 0}
        UniqueKeyHash = {}
        columns = self.tables[tableName].columns
        for i, column in enumerate(columns):
            if column.isUnique:
                UniqueKeyHash[column.columnName] = i
        return UniqueKeyHash

    def getKeyIndex(self, tableName, keyName):
        columns = self.tables[tableName].columns
        for i, column in enumerate(columns):
            if column.columnName == keyName:
                return i
        return -1

    def getUniqueKeyColumns(self, tableName):
        uniqueKeyColumns = []
        columns = self.tables[tableName].columns
        for column in columns:
            if column.isUnique:
                uniqueKeyColumns.append(column)
        return uniqueKeyColumns