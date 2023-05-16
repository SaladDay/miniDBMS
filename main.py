from interpreter import Interpreter

if __name__ == '__main__':
    interpreter = Interpreter()
    interpreter.cmdloop() 

# TEST:

'''''
create table stu (
    sno char(8), 
    sname char(16) unique, 
    sage int, 
    sgender char(1), 
    primary key (sno)
);

create table stu(sno char(8), sname char(16) unique, sage int, sgender char(1), primary key (sno))
insert into stu values ('12345678', 'wy', 22, 'M');
insert into stu values ('12348', 'wy', 22, 'M');

create table S(ID int, name char(12) unique, age int, gender char(1), primary key (ID));
insert into S values(1,'stz',20,'M');
但上面这个不会。

TESTBENCH:
create table stu(sno char(8), sname char(16) unique, sage int, sgender char(1), primary key (sno))
insert into stu values ('12345678', 'wy', 22, 'M');
insert into stu values ('123', 'wdsa', 21, 'F');
insert into stu values ('114514', 'ads', 20, 'M');
insert into stu values ('11452', 'cca', 64, 'M');
insert into stu values ('1', 'ccf', 20, 'F');
insert into stu values ('12', 'oop', 30, 'T');
insert into stu values ('113', 'noi', 50, 'S');

create table S(ID int, name char(12) unique, age int, gender char(1), primary key (ID));
insert into S values(1,'stz',20,'M');
insert into S values(2,'jyc',19,'M');
insert into S values(3,'lgy',20,'M');
insert into S values(4,'fyh',19,'M');
insert into S values(5,'homura',500,'F');
insert into S values(6,'Motoka',600,'F');
insert into S values(7,'AAB',19,'F');
insert into S values(8,'AAC',17,'F');
insert into S values(9,'AAD',23,'F');
insert into S values(10,'AAE',13,'F');
insert into S values(11,'AAF',10,'M');
insert into S values(12,'AAG',10,'F');
insert into S values(13,'AAH',12,'M');
insert into S values(14,'AAI',13,'M');
insert into S values(15,'AAJ',25,'F');
insert into S values(16,'AAK',22,'M');
insert into S values(17,'AAL',18,'M');
insert into S values(18,'AAM',20,'M');
insert into S values(19,'AAN',24,'F');
insert into S values(20,'AAO',30,'M');

delete from S where ID < 18
delete from S where ID < 19
delete from S where ID < 20
delete from S where ID < 21

create index stunameidx on S ( name );

create table A(ID int, name char(12));

create index stunameidx on stu ( sname );

'''

