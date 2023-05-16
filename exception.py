class SqlSyntaxError(Exception):  # 自定义异常，不用实现多态
    pass

class QueryException(Exception):  # 查询中异常
    pass

class BPlusTreeException(Exception):  # B+树操作异常
    pass

class BufferException(Exception):  # Buffer操作异常
    pass

class IndexMgrException(Exception): 
    pass