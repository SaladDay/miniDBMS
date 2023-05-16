import sys, os
import re 
import time
from cmd import Cmd

from api import Api

from exception import BufferException, QueryException, SqlSyntaxError

# Disable print()
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

class SqlParser:
    def strAutoDataType(data):
        if data[0] == "'" and data[-1] == "'":
            return data[1:-1]
        elif re.match(r'^-?[0-9]+\.+[0-9]+$', data):
            return float(data)
        elif re.match(r'^-?[0-9]+', data):
            return int(data)
        else:
            raise SqlSyntaxError(f'抱歉，不支持 {data} 的数据类型。')

    def create(sql):
        sql = sql.strip()  # 移除首尾空格、换行符
        if len(sql) < 6:
            raise SqlSyntaxError('create 语句不合法！')

        if sql[-1] == '(':  # create table 表名 ( 多行输入标志
            while True:
                item = input().strip()
                sql += item
                if item[-1] != ',' and item[-1] != ')':
                    break
        sql = sql.strip(';').strip()
        sql = re.sub(' +', ' ', sql)  # 多个连续的空格替换为单空格

        if sql[:5] == 'table':
            return 'table', SqlParser.create_table(sql)
        elif sql[:5] == 'index':
            return 'index', SqlParser.create_index(sql)
        else:
            raise SqlSyntaxError('抱歉，本程序暂不支持创建 %s .' % sql[1])      
    
    def create_table(sql):
        sql = sql[5:].strip()
        tableName = sql[:sql.find('(')].strip()  # 获取表名
        if tableName == '':
            raise SqlSyntaxError('create table 语句不合法！')

        sql = sql[sql.find('(') : ]
        sql = sql.lstrip('(').strip()  # 去除左括号
        if sql[-1] == ')':
            sql = sql[:-1].strip()
        if sql == '':
            raise SqlSyntaxError('表的字段定义缺失！')

        attributesSpecs = sql.split(',')
        attributesSpecs = list(map(str.strip, attributesSpecs))
        if attributesSpecs == []:
            raise SqlSyntaxError('表的字段定义缺失！')
        
        # 处理主键
        primaryKey = None
        if attributesSpecs[-1].startswith('primary key'):
            primaryKey = attributesSpecs[-1]
            if ',' in primaryKey:
                raise SqlSyntaxError('不能定义多个主键！')
            primaryKey = primaryKey[11:].strip().lstrip('(').rstrip(')').strip()
            attributesSpecs = attributesSpecs[:-1]
        
        # 处理属性
        attributes = []
        for attributesSpec in attributesSpecs:
            isUnique = False
            charLen = 0  # 仅char类型有用
            item = attributesSpec.split()
            name = item[0]
            if (len(item) <= 1):
                raise SqlSyntaxError('create table 语句不合法！')
            type = item[1]  # 类型
            
            if type not in ['int', 'float']:
                if type.startswith('char'):
                    charLen = int(type[4:].strip().lstrip('(').rstrip(')'))
                    if (charLen <= 0):
                        raise SqlSyntaxError('char 类型的长度不能为负！')
                    type = 'char'
                else:
                    raise SqlSyntaxError(f'抱歉，属性 {name} 的数据类型为 {type}，本程序暂不支持该数据类型.')

            if len(item) == 3:
                if item[2] == 'unique':
                    isUnique = True
                else:
                    raise SqlSyntaxError(f'抱歉，为属性 {name} 添加的限制不合法.')

            attributes.append({
                'name': name,
                'type': type,
                'charLen': charLen,
                'isUnique': isUnique
            })

        attributeNames = [attribute['name'] for attribute in attributes]
        if primaryKey is not None:  # 设置主键为unique
            if primaryKey not in attributeNames:
                raise SqlSyntaxError(f'主键 {primaryKey} 不是表中的属性！')
            else:
                attributes[attributeNames.index(primaryKey)]['isUnique'] = True
        
        return tableName, attributes, primaryKey

    def create_index(sql):
        sql = sql[5:].strip()
        findOn = sql.find('on')
        if findOn == -1:
            raise SqlSyntaxError('create index 语句中缺失 on ！')
        
        indexName = sql[:findOn].strip()  # 定位索引名

        findLBracket = sql.find('(')
        if findLBracket == -1:
            raise SqlSyntaxError('create index 语句不合法！')
        
        tableName = sql[findOn + 2 : findLBracket].strip()  # 定位表名

        findRBracket = sql.find(')')
        if findRBracket == -1:
            raise SqlSyntaxError('create index 语句不合法！')

        attributeName = sql[findLBracket + 1 : findRBracket].strip()  # 定位被创建索引的属性名

        if ',' in attributeName:
            raise SqlSyntaxError('抱歉，本程序不支持在单个 create index 语句中为多属性创建索引')

        return indexName, tableName, attributeName

    def drop(sql):
        sql = sql.strip(';').strip()
        sql = re.sub(' +', ' ', sql)  # 多个连续的空格替换为单空格
        if sql[:5] == 'table':
            return 'table', SqlParser.drop_table(sql)
        elif sql[:5] == 'index':
            return 'index', SqlParser.drop_index(sql)
        else:
            raise SqlSyntaxError('抱歉，本程序暂不支持删除 %s .' % sql[1])      
    
    def drop_table(sql):
        sql = sql[5:].strip()
        tableName = sql[:]
        return tableName

    def drop_index(sql):
        sql = sql[5:].strip()
        indexName = sql
        return indexName

    def select(sql):
        sql = sql.strip(';').strip()
        sql = re.sub(' +', ' ', sql) 

        findFrom = sql.find('from')
        if findFrom == -1:
            raise SqlSyntaxError('select 语句中缺失 from！')
        attributeNames = sql[:findFrom].strip().split(',')  # 定位属性名
        attributeNames = list(map(str.strip, attributeNames))
        
        findWhere = sql.find('where')
        if findWhere == -1:  # 没有where
            tableName = sql[findFrom + 4 : ].strip()
            if tableName == '':
                raise SqlSyntaxError('select 语句中缺失表名！')
            return tableName, attributeNames, None
        else:
            tableName = sql[findFrom + 4 : findWhere].strip()
            conditions = sql[findWhere + 5 :].strip().split('and')
            conditions = list(map(str.strip, conditions))
            wheres = []
            for condition in conditions:
                operators = ['<>', '!=', '==', '<=', '>=', '=', '<', '>']
                #  长度为2的operator必须写在前面，否则有bug
                noOperator = True  # 该约束没有运算符
                for operator in operators:
                    if operator in condition:
                        curOperator = operator
                        curOperatorPos = condition.find(curOperator)
                        lVal = condition[: curOperatorPos].strip()
                        rVal = condition[curOperatorPos + len(curOperator) : ].strip()
                        noOperator = False
                        break  # 找到了当前条件表达式的运算符
                if noOperator:
                    raise SqlSyntaxError('抱歉，where子句中的每个and隔开的条件表达式中必须含有运算符')
                rVal = SqlParser.strAutoDataType(rVal)  # Python 变量生命周期允许这么干
                wheres.append({
                    'operator': operator,
                    'lVal': lVal,
                    'rVal': rVal
                })
            return tableName, attributeNames, wheres
    
    def insert(sql):
        sql = sql.strip(';').strip()
        sql = re.sub(' +', ' ', sql)

        findInto = sql.find('into')
        if findInto == -1:
            raise SqlSyntaxError('delete 语句中缺失 into ！')

        findValues = sql.find('values')
        if findValues == -1:
            raise SqlSyntaxError('delete 语句中 缺失 values ！')
        
        tableName = sql[findInto + 4 : findValues].strip()

        findLBracket = sql.find('(')
        findRBracket = sql.find(')')
        values = sql[findLBracket + 1 : findRBracket].split(',')
        values = list(map(str.strip, values))
        values = list(map(SqlParser.strAutoDataType, values))

        return tableName, values

    def delete(sql):
        sql = sql.strip(';').strip()
        sql = re.sub(' +', ' ', sql) 

        findFrom = sql.find('from')
        if findFrom == -1:
            raise SqlSyntaxError('delete 语句中缺失 from！')
        
        findWhere = sql.find('where')
        if findWhere == -1:  # 没有where
            tableName = sql[findFrom + 4 : ].strip()
            if tableName == '':
                raise SqlSyntaxError('select 语句中缺失表名！')
            return tableName, None
        else:
            tableName = sql[findFrom + 4 : findWhere].strip()
            conditions = sql[findWhere + 5 :].strip().split('and')
            conditions = list(map(str.strip, conditions))
            wheres = []
            for condition in conditions:
                operators = ['<>', '!=', '==', '<=', '>=', '=', '<', '>']
                noOperator = True  # 该约束没有运算符
                for operator in operators:
                    if operator in condition:
                        curOperator = operator
                        curOperatorPos = condition.find(curOperator)
                        lVal = condition[: curOperatorPos].strip()
                        rVal = condition[curOperatorPos + len(curOperator) : ].strip()

                        noOperator = False
                        break  # 找到了当前条件表达式的运算符
                if noOperator:
                    raise SqlSyntaxError('抱歉，where子句中的每个and隔开的条件表达式中必须含有运算符')
                rVal = SqlParser.strAutoDataType(rVal)  # Python 变量生命周期允许这么干
                wheres.append({
                    'operator': operator,
                    'lVal': lVal,
                    'rVal': rVal
                })
            return tableName, wheres

    def show(sql):
        sql = sql.strip(';').strip()
        sql = re.sub(' +', ' ', sql)

        findTables = sql.find('tables')
        if findTables == -1:
            findTable = sql.find('table')
            if findTable == -1:
                raise SqlSyntaxError('抱歉，本程序只支持 "show tables" 和 "show table 表名" 两种 show 语句')
            tableName = sql[findTable + 5 : ].strip()
            return 'table', tableName
        return 'tables', None


class Interpreter(Cmd):  # 交互式shell
    prompt = 'MiniSQL>> '
    intro = 'MiniSQL for DB lab. Welcome...'

    def __init__(self):
        Cmd.__init__(self)
        path = os.getcwd()  # 当前工作目录
        self.api = Api(path)  # 成员变量

    def do_create(self, arg):  
        try:
            returnVal = SqlParser.create(arg)
            if returnVal[0] == 'table':
                self.api.createTable(returnVal[1][0], returnVal[1][1], returnVal[1][2])
            elif returnVal[0] == 'index':
                self.api.createIndex(returnVal[1][0], returnVal[1][1], returnVal[1][2])
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)

    def do_drop(self, arg):
        try:
            returnVal = SqlParser.drop(arg)
            if returnVal[0] == 'table':
                self.api.dropTable(returnVal[1])
            elif returnVal[0] == 'index':
                self.api.dropIndex(returnVal[1])
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)
        '''''
        drop table student;
        drop index stunameidx;
        '''

    def do_select(self, arg):
        try:
            returnVal = SqlParser.select(arg)
            if returnVal[2] == None:
                self.api.select(returnVal[0], returnVal[1], dict())
            else:
                self.api.select(returnVal[0], returnVal[1], returnVal[2])
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)
        '''''
        select * from student;
        select * from student where sno = '88888888';
        select * from student where sage > 20 and sgender = 'F';
        '''

    def do_insert(self, arg):
        try:
            returnVal = SqlParser.insert(arg)
            self.api.insert(returnVal[0], returnVal[1])
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)
        '''''
        insert into student values ('12345678', 'wy', 22, 'M');
        '''

    def do_delete(self, arg):
        try:
            returnVal = SqlParser.delete(arg)
            if returnVal[-1] == None:
                self.api.delete(returnVal[0], dict())
            else:
                self.api.delete(returnVal[0], returnVal[1])
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)
        '''''
        delete from student;
        delete from student where sno = '88888888';
        '''

    def do_show(self, arg):
        try:
            returnVal = SqlParser.show(arg)
            if returnVal[0] == 'table':
                self.api.show(returnVal[1])
            elif returnVal[0] == 'tables':
                self.api.show(None)
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)
        except QueryException as queryExcepetion:
            print(queryExcepetion)
        '''
        show tables
        show table student
        '''

    def do_execfile(self, arg): 
        # 本execfile每行读入一个sql语句，即不允许单个sql语句占据多行
        
        startTime = time.time()

        try:
            blockPrint()  # execfile时禁用print()
            with open(arg, 'r') as f:
                while True:
                    line = f.readline().strip()
                    if line == '':
                        break
                    if line[0] == '-':  # 注释
                        continue
                    seperator = line.find(' ')
                    sqlCommand = line[ : seperator]
                    switch = {
                        'create': self.do_create,
                        'drop': self.do_drop,
                        'select': self.do_select,
                        'insert': self.do_insert,
                        'delete': self.do_delete,
                        'show': self.do_show
                    }
                    sql = line[seperator : ]
                    switch[sqlCommand](sql)
            enablePrint()  # 恢复
            endTime = time.time()
            print(f'执行文件操作用了 {(endTime - startTime)} 秒。')
        except FileNotFoundError as fileNotFoundError:
            print(fileNotFoundError)
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)

    def do_quit(self, arg):
        try:
            self.api.save()
            self.stdout.write('所有对数据库的更改已保存。\n')
            self.stdout.write('Bye')
            return True
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)
    
    def do_exit(self, arg):
        return self.do_quit(arg)

    def do_commit(self, arg):
        try:
            self.api.save()
            self.stdout.write('所有对数据库的更改已保存。\n')
        except SqlSyntaxError as syntaxErr:
            print(syntaxErr)

    def default(self, line):  # 重写的父类方法
        self.stdout.write('*** 未知的 SQL 语法: %s\n' % line.split()[0])