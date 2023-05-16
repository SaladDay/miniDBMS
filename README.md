# 任务

针对关系型数据库SQL语句执行，进行需求分析，并设计一个简单的数据库系统SQL执行器模块，根据设计模拟实现SQL基本操作等，主要实现：数据库创建、表格创建、数据添加、删除、更新、查询等操作。

模拟实现采用：python或者Java实现具体功能，设计中若有数据的存储可以使用文本文件或者excel文件，SQL语句执行采用函数实现。

# 设计

## 基本定义

- 数据类型:支持三种基本数据类型:integer，char(n)，float。其中 1 ≤ n ≤ 255。

- 表定义:一个表可以定义至多 32 个属性，各属性可以指定是否为 unique;支持 单属性的主键定义(多属性未支持)。

- 索引定义:对于表的主属性自动建立 B+树索引，对于声明为 unique 的属性可以通过 SQL 语句由用户指定建立/删除 B+树索引(因此，所有的 B+ 树索引都是单属性且单值的)。
- 数据操作: 可以通过指定用 and 连接的多个条件进行查询，支持等值查询和区间查询。支持每次一条记录的插入操作;支持每次一条或多条记录的删除操作。
- 支持的SQL语句：
  - create table
  - drop table
  - create index（出BUG）
  - drop index（出BUG）
  - select from where
  - insert into
  - delete from 
  - Execfile
  - quit
  - show tables
  - show table
  - commit

## 系统架构

![image-20230516111435634](https://saladday-figure-bed.oss-cn-chengdu.aliyuncs.com/img/image-20230516111435634.png)



### interpreter模块

前端和简单的逻辑处理。实现一个交互性 Shell，对正确的 SQL 语句，调用 API模块的方法，执行并给出相应的结果 ; 对于错误的语句，给出错误信息。

PS.在interpreter模块中还附加了使用正则表达式实现的sql_parser

### API模块

核心模块。为 Interpreter 提供 SQL 语句的接口，并根据 Catalog Manager 提供 的信息确定执行规则，并调用 Record Manager、Index Manager 和 Catalog Manager 提供的相应接口进行执行，最后返回执行结果给 Interpreter 模块。相当于Web应用中的controller

### Catalog Manager 模块

meta data。

其中数据包括：数据库中所有表的定义(表的名称、表中字段(列)数、主键、定义在 该表上的索引)，表中每个字段的信息(字段类型、是否唯一等)，数据库中所有索引的信息(包括所属表、索引在某字段等)。

Catalog Manager提供了访问及操作此信息的接口，供API使用。

### Index Manager 模块

B+ 树数据结构的具体实现。

Index Manager提供了索引操作的相关接口。索引使用B+树定义。

### Buffer Manager 模块

Buffer Manager 管理记录表中数据的数据文件。

主要功能为实现数据文件的创建与删除(由表的定义与删除引起)、记录的插入、删除与查找操作，并提供相应的接口。

**PS：数据文件的定义：**

本系统采用二进制方式存储记录(和PostgreSQL类似)。

数据文件由一个或多个数据块组成（为提高磁盘 I/O 操作的效率，缓冲区与文件系统交互的单位是块，块的大小应为 文件系统与磁盘交互单位的整数倍，一般可定为 4KB 或 8KB），快大小与缓冲区大小相同。一个块中包含至多k条记录，其中k可以根据操作系统的文件管理规则调整设定，不支持记录的跨块存储。

主要功能有：

1. 根据需要，读取指定的数据到系统缓冲区或将缓冲区中的数据写出到文件
2. 实现缓冲区块的替换算法，当缓冲区块满时调整其偏移量。
3. 记录缓冲区中各块的状态，如是否被修改过等。
4. 提供缓冲区块的 pin 功能，即锁定缓冲区的块，不允许调整出去。

### files

包括IndexFile,CatelogFile,RecordFile。其中IndexFile,CatelogFile采用Json的格式定义。RecordFile采用**自创的**二进制格式定义。

- CatelogFile/tableCatelog.json--表定义相关信息

  ```
  {
  	"stu": {
  		"columns": {
  			"id": {
  				"isUnique": true,
  				"type": "int",
  				"charLen": 0
  			},
  			"name": {
  				"isUnique": true,
  				"type": "char",
  				"charLen": 16
  			},
  			"gpa": {
  				"isUnique": false,
  				"type": "float",
  				"charLen": 0
  			},
  			"age": {
  				"isUnique": false,
  				"type": "int",
  				"charLen": 0
  			}
  		},
  		"primaryKey": "id"
  	},
  	"shit": {
  		"columns": {
  			"sno": {
  				"isUnique": true,
  				"type": "int",
  				"charLen": 0
  			},
  			"sname": {
  				"isUnique": false,
  				"type": "char",
  				"charLen": 16
  			}
  		},
  		"primaryKey": "sno"
  	}
  }
  
  ```

- CatelogFile/indexCatelog.json--索引的元数据信息

  出bug了，没出来



- IndexFile/indexFile.json--B+树的存储

  ```
  {
  	"stu": {
  		"id": {
  			"keys": [106003, 106007, 106009],
  			"isLeaf": false,
  			"children": [{
  				"keys": [106000, 106001, 106002],
  				"isLeaf": true,
  				"children": [
  					[990],
  					[991],
  					[992]
  				]
  			}, {
  				"keys": [106003, 106004, 106005],
  				"isLeaf": true,
  				"children": [
  					[993],
  					[994],
  					[995]
  				]
  			}, {
  				"keys": [106007, 106008],
  				"isLeaf": true,
  				"children": [
  					[997],
  					[998]
  				]
  			}, {
  				"keys": [106009, 106010],
  				"isLeaf": true,
  				"children": [
  					[999],
  					[1000]
  				]
  			}]
  		}
  	},
  	"shit": {
  		"sno": {
  			"keys": [123],
  			"isLeaf": true,
  			"children": [
  				[1]
  			]
  		}
  	}
  }
  ```

  

## 二进制文件管理

本系统采用二进制的方式定义记录文件。

对于单个表emp而言，其所有的记录都会被存储到一个单Record文件内，且一个表对应一个文件。二进制文件中，每min(1+k,5)个字节小端存储一个真实的记录或者空洞链表节点，其中k是该表定义的各个字段所需的存储字节的大小之和。下面，我们介绍该文件是如何保存记录的，并处理插入和删除操作的。

》**文件内容审视**：用010Editor或类似的二进制文件查看器，打开之前执行过miniSQL语句的表emp的记录文件./records/emp.dat,其内容如图：

![image-20230516155714546](https://saladday-figure-bed.oss-cn-chengdu.aliyuncs.com/img/image-20230516155714546.png)

其中我们emp表的定义为:

```sql
create table emp(eno int,ename char(10),salary float,depart char(6),primary key (eno))
```

由于int,char(10),float,char(6)各需要4,10,4,6个字节进行保存，故k=14，加上标志位，故每25个字节都存储一个真实的记录或者空洞链表节点。

可见，文件的第一个25个字节并且真实的记录，其他的25个字节都是真实的记录，我们用每个记录的第一个字节来标识该字节段的类型：

- 若第一个字节为0，则为真实的记录；
- 若第一个字节为1，则为空洞链表节点；

若已经确定了第一个字节为1，则该25个字节中，第二第三第四第五个字节（总共4字节）都用来保存空洞链表节点的next指针。在本例中，该指针的值为06 00 00 00 H(小端)，对应十进制为6，在这里，6的含义就是“下一条插入的记录应该插入到第6个位置，即对应顺序插入”。

下面，我们试着进行删除，来看看这个空洞链表节点是如何工作的。执行的SQL语句。

```
delete from emp where eno = 125
```

将更改写入文件后，重新加载emp.dat，文件内容变化如图。

![image-20230516160504094](https://saladday-figure-bed.oss-cn-chengdu.aliyuncs.com/img/image-20230516160504094.png)

可见，eno为125的记录已经被正确删除，留下一个空洞链表节点。同时，文件开头的空洞链表指针节点的值被更改为03H，恰与该被删除记录的物理地址相同。这里，3的含义为“下一个插入的记录应该插入到第3个位置，即插入刚刚删去的空洞”。

各空洞链表结点的指针的值也在帮助头结点的指针的值动态改变，以维护一个空洞链表，确保空间的利用率。

## 原理解析

> ### 由于篇幅，不详细展开。详细将insert和select方法。
>
> **插入（INSERT）的过程如下。**
>
> 1. 判断查询目标表是否存在，若否，抛出异常；
> 2. 判断给定插入值的values数目是否与表的字段数目一致且类型匹配，若否，则抛出异常；
> 3. 判断给定插入值的char类型values是否超出了该字段上定义的charlen，若是，则抛出异常。
> 4. 将插入值的各unique value按照是否建立了索引分为两种：一种对应到已经建立了索引的字段，记为uniqueKeyWithIndex，另一种没有对应到已经建立了索引 的字段，记为uniqueKeyNotWithIndex：
>    1. 若 uniqueKeyWithIndex不为空且 uniqueKeyNotWithIndex为空，则只 需要在 B+ 树上检查插入的 value 是否满足唯一性。该检查仅耗费 O(log n) 时间复杂度。
>    2. 若 uniqueKeyWithIndex不为空且 uniqueKeyNotWithIndex不为空，则 除了在 B+ 树检查 uniqueKeyWithIndex 的插入外，还必须进行 Buffer- Mgr 提供的全表扫描检查 uniqueKeyNotWithIndex 的唯一性。
> 5. 将记录插入到BufferMgr中。
> 6. 将记录插入到IndexMgr中。
>
> **查询(SELECT)的过程如下。**
>
> 1. 判断查询目标表是否存在，若否，则抛出异常;
> 2. 判断查询目标字段是否存在，若否，则抛出异常;
> 3. 将查询的 where 子句中的各条件分成两种类型:一种是已经创建了 B+ 树索引 的字段上的查询条件，记为 uniqueKeyWheres，另一种是未创建 B+ 树索引的 字段上的查询条件，记为 notUniqueKeyWheres。分别进行如下操作:
>    1. 对于 uniqueKeyWheres，从 IndexMgr 中获取到它们对应的 B+ 树索引， 快速查询返回叶子结点的数据(即记录的物理地址);
>    2. 对于 notUniqueKeyWheres，无法访问 B+ 树索引，只能等待满足 uniqueKeyWheres 的记录查询完毕。
> 4. 随后，在 BufferMgr 提供的接口中进行记录获取。
>    1. 若 notUniqueKeyWheres 为空且 uniqueKeyWheres 不为空，则直接通过记录的物理地址得到相应的记录。
>    2. 若notUniqueKeyWheres不为空且uniqueKeyWheres为空，则必须通过全表扫描，逐一判断的方法得到相应记录；
>    3. 若两者皆不为空，则仍必须进行一次全表扫描，并与 uniqueKeyWheres 得到的记录的物理地址取交集(and)，得到最终符合查询条件的记录。
> 5. 最后，调用 ResultPrinter 的 printSelect() 方法，打印查询结果。
>
> ### B-Tree
>
> 本系统索引采用B-Tree的数据结构。
>
> 我们通过定义三个类:Node, LeafNode, BPlusTree 来实现了 B+ 树。Index Manager 在新建索引时，只需实例化一个 BPlusTree 类的对象即可。随后，在对索引 的增删改查操作，也是通过对该对象提供的接口进行的。
>
> BPlusTree 类提供的接口如下:
>
> ```python
> def __init__(self, order=4):
> def find(self, key):
> def insert(self, key, value): 
> def delete(self, key):
> def getAllData(self): 
> ```
>
> 在实例化 BPlusTree 类的对象时，会初始化其成员变量 self.root 为一 LeafN- ode 类的对象，即空叶子结点。LeafNode 类继承了 Node 类，而 LeafNode 和 Node 均为 BPlusTree 的各成员方法提供了相应的接口，在此不再过多赘述。
>
> ### 缓冲区的设计
>
> 由于对磁盘文件的读写一般比较满，所以缓冲区的设计是必要的。
>
> 在本系统中，我们用 BufferMgr 类中定义的 Buffer 类的实例化来实现缓 冲区中的“块”。一个表对应一个数据文件，它只有一个块可供“缓存”，且该块的位置(offset)可以动态改变。一个块中包含至多bufferSize条记录，其中bufferSize可以根据操作系统的文件管理规则调整设定。
>
> 简单来说，一个缓冲区的块是它对应的 Record File 的一个映射，如图7.12所示。
>
> 在实际应用中，块的 curSize 有一个上限 bufferSize，且常常需要调节块的 offset 来适应表的 CRUD 需求。
>
> 我们定义一个块的覆盖范围(range)为 range(offset, offset + curSize)。 可见，如果一个 CRUD 操作涉及到的记录的地址处在该范围内，则它可以被较迅速地 完成，否则需要进行文件读写，消耗更多时间。
>
> 在本程序中，我们参考了 Cache 的 write-allocate 和 write-back 法，会保证插入 操作一定在块内完成，即插入操作一定只写入 Buffer，不会写入文件。为此，当插入 记录的地址不处于块的覆盖范围时，需要进行调整 offset(调块)的操作。对于查 询、删除操作，我们不会移动块的 offset，仅仅被动地希望操作涉及到的记录能够 更多地处于块的覆盖范围内。
>
> 当块的内容与文件不一致时，该块记为 dirty，在调整操作之前必须先进行保存; 而在查询操作时，该块中的内容将成为查询考虑的特例。
>
> ![image-20230516162002328](https://saladday-figure-bed.oss-cn-chengdu.aliyuncs.com/img/image-20230516162002328.png)
>
> 

一个Buffer会在BufferMgr调用load()或createTable()时进行实例化。在实例化的时候，会进行一下操作。

![image-20230516162152587](https://saladday-figure-bed.oss-cn-chengdu.aliyuncs.com/img/image-20230516162152587.png)

根据 BufferMgr 的调用情况，Buffer 类向外提供如下接口:

```python
def getBufferRange(self):
def save(self):
def adjustOffset(self, offset):
def isFull(self):
def decodeRecord(self, record):
def checkWheres(record, columnHash, wheres):
def bPlusFindRecords(self, uniqueKeyResultAddrs):
def scanFindRecords(self, columnHash, notUniqueKeyWheres): def findRecords(self, columnHash, notUniqueKeyWheres,
uniqueKeyResultAddrs):
def deleteRecords(self, correspondingAddrs):
def checkUnique(self, record, columns, uniqueKeyNotWithIndexColumns
):
def insertRecord(self, values, columns,
uniqueKeyNotWithIndexColumns):
```

具体功能的讲解见源代码注释。

## 测试

### SQL语句测试

“这个地方你补全一下，我没写全。在第一小节中对我之前前面目录提供的全部sql语句进行测试，出BUG的就别测试了。”



### 性能测试

定义表stu如下：

```sql
create table stu ( 
	id int,
	name char(16) unique,
	gpa float,
	age int,
	primary key (id)
);
```

我编写了python脚本，随机生成对表stu的测试SQL语句。

![image-20230516162736046](https://saladday-figure-bed.oss-cn-chengdu.aliyuncs.com/img/image-20230516162736046.png)

该脚本会在当前工作目录下产生 test.sql 文件，包含表的 create table 语 句和紧接着的 testSize 个 insert 语句。例如，当 testSize = 5 时，可能产生的测试语句如图:

![image-20230516162843418](https://saladday-figure-bed.oss-cn-chengdu.aliyuncs.com/img/image-20230516162843418.png)

以下的测试环境均为作者的笔记本电脑：操作系统为MacOS，CPU为M1。

同时屏蔽了向终端显示结果的操作（显示会占用大量时间）

对于 testSize = 1000, 3000, 5000, 7500, 10000 的情况，我们统计了 miniSQL 的运行时间。

| 插入语句的数目 | 1000 | 3000  | 5000  | 7500  | 10000|
| -------------- | ---- | ----- | ----- | ----- |-----|
| 运行时间/s     | 0.92 | 3.51 | 6.15 | 12.796 |15.9090|

查询10000条数据，耗费时间205.3396701812744 毫秒。
 
由于篇幅，其余操作的性能测试略。
