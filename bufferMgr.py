import os
import struct
from exception import BufferException
# struct.unpack() 用来解释读取到的二进制串
# struct.pack() 用来将数据转换为二进制串

def string2Bytes(values):
    for i, value in enumerate(values):  # string转为bytes
        if isinstance(value, str):
            values[i] = values[i].encode('utf-8')

class BufferMgr:  # PLUS RecordMgr
    class Buffer:
        # TODO: 目前只实现了一个表一个buffer，未来扩展为一个表多个buffer
        bufferSize = 1024  # 每个buffer最多可以有几个record
        bufferNum = 1  # 每个表最多有几个buffer（暂时无用）
        def __init__(self, path, tableName, columns):
            self.tableName = tableName
            self.offset = 0  # buffer在磁盘文件中的位置索引
            self.isDirty = False  # 是否被改写，与文件内容不符了
            self.pinned = False  # 是否被锁定
            self.formatList = ['<c']  # 小端，char
            self.recordSize = 1  # 单个record占用多少Byte
            #  record: 0, pointer: 1。尽管是布尔变量，但还是用了1 Byte来存储

            for column in columns:
                if column.type == 'int':
                    self.formatList += ['i']
                    self.recordSize += 4
                elif column.type == 'float':
                    self.formatList += ['f']
                    self.recordSize += 4
                elif column.type == 'char':  # 's' for str
                    self.formatList += [f'{column.charLen}s']
                    self.recordSize += column.charLen
            if self.recordSize < 5:
                self.recordSize = 5  # 至少得有5个byte

            self.curSize = 1
            self.content = []

            self.recordFile = os.path.join(path, f'{self.tableName}.dat')
            self.adjustOffset(self.offset)

            ### 关键代码
            self.insertPos = struct.unpack(
                '<I', 
                self.content[0][1:5]
            )[0]  # 小端，integer or long

        def getBufferRange(self):
            return range(self.offset, self.offset + self.curSize)

        def save(self):
            if self.isDirty is True:
                with open(self.recordFile, 'rb+') as recordFile:
                    recordFile.seek(self.offset * self.recordSize)  # 移动文件指针
                    for record in self.content:
                        recordFile.write(record)
                        # print('把一个脏记录写入文件！')

                    recordFile.seek(0)  # 改空洞链表的表头
                    recordFile.write(struct.pack(
                        f'<cI{self.recordSize - 5}s', 
                        b'\x01',  # 是空洞链表的node
                        self.insertPos,  # 存储插入位置
                        b'\x00' * (self.recordSize - 5)  # 后面全是0
                    ))
                self.isDirty = False

        def adjustOffset(self, offset):
            # adjust()
            if self.pinned is True:
                raise BufferException('Buffer操作异常：不允许调整被锁定的Buffer！')
            elif self.isDirty is True:
                self.save()

            self.content = []
            with open(self.recordFile, 'rb') as recordFile:
                recordFile.seek(offset * self.recordSize)
                for i in range(self.bufferSize):
                    record = recordFile.read(self.recordSize)
                    if record == b'':  # 什么也读不出来(EOF)，该停止了。
                        self.curSize = i  # 获取当前大小
                        break
                    self.content.append(record)
            self.offset = offset

        def isFull(self):
            return self.curSize == self.bufferSize

        def decodeRecord(self, record):  # 一个二进制record解码成正常数据
            record = list(struct.unpack(''.join(self.formatList), record))
            for i, item in enumerate(record):
                if isinstance(item, bytes):
                    record[i] = record[i].decode('utf-8').rstrip('\x00')  # 规范的python str
            
            return record[1:]  # 第一个Byte作为标志不要返回
            # 返回值示例：[19, 'AAN', 24, 'F']

        # operators = ['<>', '!=', '=', '==', '<=', '>=', '<', '>']
        @staticmethod
        def checkWheres(record, columnHash, wheres):
            if wheres == []:
                return True

            for where in wheres:
                columnIndex = columnHash[where['lVal']]
                lVal = record[columnIndex]
                rVal = where['rVal']
                operator = where['operator']
                if operator == '<>' or operator == '!=':
                    if lVal == rVal:
                        return False
                elif operator == '=' or operator == '==':
                    if lVal != rVal:
                        return False
                elif operator == '<=':
                    if not lVal <= rVal:
                        return False
                elif operator == '>=':
                    if not lVal >= rVal:
                        return False
                elif operator == '<':
                    if not lVal < rVal:
                        return False
                elif operator == '>':
                    if not lVal > rVal:
                        return False
                else:
                    raise BufferException('where 子句中出现了不支持的运算符！')
            return True

        def bPlusFindRecords(self, uniqueKeyResultAddrs):
            # 不用全表扫描，只有B+索引
            results = []  # 查找出来的记录
            correspondingAddrs = []  # 每个记录的地址。
            with open(self.recordFile, 'rb') as recordFile:
                recordFile.seek(self.recordSize)
                for addr in uniqueKeyResultAddrs:  
                    # B+树结果
                    if addr in self.getBufferRange():
                        record = self.content[addr - self.offset]
                        record = self.decodeRecord(record)
                        results += [record]
                        correspondingAddrs.append(addr)
                    else:
                        recordFile.seek(self.recordSize * addr)
                        record = recordFile.read(self.recordSize)
                        record = self.decodeRecord(record)
                        results += [record]
                        correspondingAddrs.append(addr)
            return results, correspondingAddrs

        def scanFindRecords(self, columnHash, notUniqueKeyWheres):
            # 全表扫描
            results = []  # 查找出来的记录
            correspondingAddrs = []  # 每个记录的地址

            with open(self.recordFile, 'rb') as recordFile:
                recordFile.seek(self.recordSize)
                addr = 0
                while True:
                    addr += 1  # buffer在磁盘文件中的位置索引
                    if addr in self.getBufferRange():  # buffer hit。直接从buffer里面读取即可
                        for record in self.content:
                            if record[0] == 1:
                                continue
                            record = self.decodeRecord(record)
                            if self.checkWheres(record, columnHash, notUniqueKeyWheres):
                                results += [record]
                                correspondingAddrs.append(addr)
                        addr = self.getBufferRange()[-1]
                        recordFile.seek(self.recordSize * (addr + 1))  #  由于只实现了1个Buffer，所以接下来就是文件扫描了
                    else:  # buffer not hit. 需要从文件中读取
                        # not hit 也不会写 buffer
                        record = recordFile.read(self.recordSize)  # read()会自动移动文件指针
                        if record == b'':  # EOF
                            break
                        elif record[0] == 1:  # 空洞链表，往后
                            continue
                        record = self.decodeRecord(record)
                        if self.checkWheres(record, columnHash, notUniqueKeyWheres):
                            results += [record]
                            correspondingAddrs.append(addr)

            return results, correspondingAddrs

        def findRecords(self, columnHash, notUniqueKeyWheres, uniqueKeyResultAddrs):
            #  全表扫描 + B+索引都有
            results = []  # 查找出来的记录
            correspondingAddrs = []

            with open(self.recordFile, 'rb') as recordFile:
                recordFile.seek(self.recordSize)
                addr = 0
                while True:
                    addr += 1  # buffer在磁盘文件中的位置索引
                    if addr in self.getBufferRange():  # buffer hit。直接从buffer里面读取即可
                        for record in self.content:
                            if record[0] == 1:
                                continue
                            record = self.decodeRecord(record)
                            if self.checkWheres(record, columnHash, notUniqueKeyWheres) and addr in uniqueKeyResultAddrs:
                                results += [record]
                                correspondingAddrs.append(addr)
                        addr = self.getBufferRange()[-1]
                        recordFile.seek(self.recordSize * (addr + 1))  #  由于只实现了1个Buffer，所以接下来就是文件扫描了
                    else:  # buffer not hit. 需要从文件中读取
                        # not hit 也不会写 buffer
                        record = recordFile.read(self.recordSize)

                        if record == b'':  # EOF
                            break
                        elif record[0] == 1:  # 空洞链表，往后
                            continue
                        else:
                            record = self.decodeRecord(record)
                            if self.checkWheres(record, columnHash, notUniqueKeyWheres) and addr in uniqueKeyResultAddrs:
                                results += [record]
                                correspondingAddrs.append(addr)

            return results, correspondingAddrs

        def deleteRecords(self, correspondingAddrs):
            #  直接用指针删
            with open(self.recordFile, 'rb+') as recordFile:
                for addr in correspondingAddrs:
                    if addr in self.getBufferRange():  # buffer中删除
                        self.content[addr - self.offset] = struct.pack(  # 删掉，变成空洞链表的结点
                            f'<cI{self.recordSize - 5}s',
                            b'\x01', 
                            self.insertPos,
                            b'\x00' * (self.recordSize - 5)
                        )
                        self.insertPos = addr  # 链表头结点的指针指向刚刚删掉的位置
                        self.isDirty = True  # buffer与文件的内容不统一了
                    else:
                        recordFile.seek(self.recordSize * addr)
                        recordFile.write(struct.pack(
                            f'<cI{self.recordSize - 5}s',
                            b'\x01', 
                            self.insertPos,
                            b'\x00' * (self.recordSize - 5)                                    
                        ))
                        self.insertPos = addr
                # recordFile.flush()

        def checkUnique(self, record, columns, uniqueKeyNotWithIndexColumns):
            # 全表扫描检查插入记录是否违反字段定义的唯一性
            # API自行验证uniqueKeyNotWithIndex是否是unique的
            uniqueBytesIndex = []
            i = 1
            for column in columns:
                if (column.type == 'int' or column.type == 'float'):
                    if column in uniqueKeyNotWithIndexColumns:
                        uniqueBytesIndex += [(i, i + 4)]
                    i += 4
                elif column.type == 'char':
                    if column in uniqueKeyNotWithIndexColumns:
                        uniqueBytesIndex += [(i, i + column.charLen)]
                    i += column.charLen

            if self.isDirty:  # Buffer Dirty了，必须先在里面搜
                for existedRecord in self.content:  # 与已存在的数据作比较
                    if existedRecord[0] == 1:
                        continue
                    for i, j in uniqueBytesIndex:
                        if existedRecord[i : j] == record[i : j]:
                            raise BufferException(f'记录插入失败，原因它违反了表中某字段定义的唯一性')

            with open(self.recordFile, 'rb') as recordFile:
                recordFile.seek(self.recordSize)
                index = 0
                while True:
                    index += 1
                    if index in self.getBufferRange() and self.isDirty:  # 查到了在Buffer里的记录，可以跳过（注意：必须Dirty）
                        index = self.getBufferRange()[-1]
                        recordFile.seek(self.recordSize * (index + 1))
                    else:  # not hit
                        existedRecord = recordFile.read(self.recordSize)
                        if existedRecord == b'':
                            break
                        elif existedRecord[0] == 1:
                            continue
                        else:
                            for i, j in uniqueBytesIndex:
                                if existedRecord[i : j] == record[i : j]:
                                    raise BufferException(f'记录插入失败，原因它违反了表中某字段定义的唯一性')

        def insertRecord(self, values, columns, uniqueKeyNotWithIndexColumns):
            string2Bytes(values)

            record = b''
            record += struct.pack('<c', b'\x00')
            for i, value in enumerate(values):
                try:
                    record += struct.pack(self.formatList[i + 1], value)  # i + 1，第一个是真假record标记，不用管
                except struct.error as structError:
                    print(structError)
                    print(f'检测到变量 {value} 无法打包成二进制，请检查整型类型是否溢出等类似问题！')
                    exit(1)
            # record = 读取到的二进制record

            if uniqueKeyNotWithIndexColumns != []:
                print('进入全表扫描')
                self.checkUnique(record, columns, uniqueKeyNotWithIndexColumns)

            if not self.insertPos in self.getBufferRange():
                self.adjustOffset(self.insertPos)
                # print('self.offset =', self.offset)

            insertPos = self.insertPos  # 新记录会在这里被插入。只插入Buffer，不插入文件
            if self.insertPos - self.offset == self.curSize:  # 如果插入的是顺序下一个空位
                self.insertPos += 1
                self.content.append(record)
                self.curSize += 1
            elif self.insertPos - self.offset < self.curSize:  # 如果插入一个空洞
                self.insertPos = struct.unpack(
                    '<cI',
                    self.content[self.insertPos - self.offset][0:5]
                )[1]  # 链表接过去
                self.content[insertPos - self.offset] = record  # 插入内容
            else:
                raise BufferException('插入到了不合法的地点！')

            self.isDirty = True
            return insertPos


    def __init__(self, path, catalogTables):
        self.path = os.path.join(path, 'dbfiles/records')
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        
        self.buffers = {}
        self.load(catalogTables)

    def save(self):
        for buffer in self.buffers.values():
            buffer.save()

    def load(self, catalogTables):
        for tableName, catalogTable in catalogTables.items():
            self.buffers[tableName] = self.Buffer(
                path=self.path,
                tableName=tableName,
                columns=catalogTable.columns
            )

    def findRecords(self, tableName, columnHash, notUniqueKeyWheres, uniqueKeyWheres, uniqueKeyResultAddrs):
        buffer = self.buffers[tableName]
        if uniqueKeyWheres == []:  # 纯全表扫描
            print('您正在进行全表扫描。')
            return buffer.scanFindRecords(
                columnHash, 
                notUniqueKeyWheres, 
            )
        elif notUniqueKeyWheres == []:  # 纯B+树
            print('您正在进行在B+树索引帮助下的查询。')
            return buffer.bPlusFindRecords(
                uniqueKeyResultAddrs
            )
        else:  #  带条件的全表扫描
            print('您正在进行在B+树索引帮助下的全表扫描。')
            return buffer.findRecords(
                columnHash, 
                notUniqueKeyWheres, 
                uniqueKeyResultAddrs
            )

    def deleteRecords(self, tableName, correspondingAddrs):
        buffer = self.buffers[tableName]
        buffer.deleteRecords(correspondingAddrs)

    def insertRecord(self, tableName, values, columns, uniqueKeyNotWithIndexColumns):
        # 注意传参
        buffer = self.buffers[tableName]
        return buffer.insertRecord(values, columns, uniqueKeyNotWithIndexColumns)

    def createTable(self, tableName, columns):  # 文件初始化一个表
        # 注意传参
        recordFile = os.path.join(self.path, f'{tableName}.dat')

        recordSize = 1
        with open(recordFile, 'wb') as recordFile:
            for column in columns:
                if column.type == 'int' or column.type == 'float':
                    recordSize += 4
                elif column.type == 'char':
                    recordSize += column.charLen
                else:
                    raise BufferException(f'抱歉，不支持 {column.columnName} 的数据类型')
            if recordSize < 5:
                recordSize = 5
            recordFile.write(struct.pack(
                f'<cI{recordSize - 5}s',
                b'\x01',
                1,  # 空洞链表的头结点（第0行）指向第1行
                b'\x00' * (recordSize - 5)
            ))
        self.buffers[tableName] = BufferMgr.Buffer(  # 一个表对应一个Buffer
            path=self.path,
            tableName=tableName,
            columns=columns
        )

    def dropTable(self, tableName):
        self.buffers.pop(tableName)
        recordFile = os.path.join(self.path, f'{tableName}.dat')
        os.remove(recordFile)

    def getInsertPos(self, tableName):
        buffer = self.buffers[tableName]
        return buffer.insertPos

if __name__ == '__main__':
    pass