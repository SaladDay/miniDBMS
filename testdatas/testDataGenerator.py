import random

createTable = 'create table stu (id int, name char(16) unique, gpa float, age int, primary key (id));\n'
sql = "insert into stu values ({0}, '{1}', {2}, {3});\n"

def charEncode(digits):
    res = ''
    while True:
        base = digits % 26
        toBeInserted = chr(base + ord('a'))
        res = toBeInserted + res
        digits //= 26
        if digits == 0:
            break
    return res

if __name__ == '__main__':
    idStart = 105011  # 当心超出int范围
    testSize = 3000

    ids = range(idStart, idStart + testSize)
    names = random.sample(range(1145, 1145141), testSize)
    gpas = []
    ages = []
    for i in range(testSize):
        gpas.append(random.uniform(0.1, 5))
        ages.append(random.randint(18, 28))

    with open('test.sql', 'w') as testFile:
        testFile.write(createTable)
        for i in range(testSize):
            testFile.write(sql.format(
                i + idStart, 
                charEncode(names[i]), 
                gpas[i], 
                ages[i]
            ))
