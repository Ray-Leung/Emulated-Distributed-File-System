#!/usr/bin/env python
# coding: utf-8

import pymysql
import pandas as pd
from sqlalchemy import create_engine



def init_database(db):
    cursor = db.cursor()
        
    sql = """
  CREATE TABLE IF NOT EXISTS metadata(
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    type varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    content text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
    PRIMARY KEY (id) USING BTREE
  ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic 
  """
    cursor.execute(sql)

    
    sql = """
  CREATE TABLE directory  (
    id int NOT NULL AUTO_INCREMENT,
    parent_id int NOT NULL,
    child_id int NOT NULL,
    PRIMARY KEY (id) USING BTREE,
    INDEX pid_metadata(parent_id) USING BTREE,
    INDEX cid_metadata(child_id) USING BTREE,
    CONSTRAINT cid_metadata FOREIGN KEY (child_id) REFERENCES metadata (id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT pid_metadata FOREIGN KEY (parent_id) REFERENCES metadata (id) ON DELETE CASCADE ON UPDATE CASCADE
  ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;
  """
    cursor.execute(sql)

    sql = """
  CREATE TABLE partitioninfo  (
    id int NOT NULL AUTO_INCREMENT,
    fid int NOT NULL,
    partition_name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    PRIMARY KEY (id) USING BTREE,
    INDEX fid_metadata(fid) USING BTREE,
    CONSTRAINT fid_metadata FOREIGN KEY (fid) REFERENCES metadata (id) ON DELETE CASCADE ON UPDATE CASCADE
  ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;
  """
    cursor.execute(sql)
    
    sql = "INSERT INTO metadata(name, type) VALUES ('%s', 'directory')" % ('root')
    cursor.execute(sql)
    db.commit()


def get_metadata(db, id: int):
    cursor = db.cursor()

    sql = "select * from metadata where id = %d" % (id)
    cursor.execute(sql)
    return cursor.fetchall()[0]


def get_max_id(db, table):
    cursor = db.cursor()

    cursor.execute('select id from %s' % table)
    res = cursor.fetchall()
    ans = 0
    for i in res:
        ans = max(ans, i[0])
    return ans


def mkdir(db, path: str):
    cursor = db.cursor()

    dirs = path.split('/')
    dirs[0] = 'root'
    dir_id = 1
    for dir in dirs:
        exist_dir = False
        sql = "select * from directory where parent_id = %d" % (dir_id)
        cursor.execute(sql)
        res = cursor.fetchall()
        for _ in res:
            #(id, pid, cid)
            metadata = get_metadata(db, _[2])
            #(id, name, type, content)
            if metadata[1] == dir and metadata[2] == 'directory':
                dir_id = metadata[0]
                if dir == dirs[-1]:
                    exist_dir = True
                    break
    if not exist_dir:
        max_id = get_max_id(db, 'metadata')
        f_name = dirs[-1]
        sql = "INSERT INTO metadata(name, type) VALUES (%s, %s)"
        val = (f_name, 'directory')
        cursor.execute(sql, val)
        db.commit()
        cid = max_id + 1
        #max_id = get_max_id('directory')
        sql = "INSERT INTO directory(parent_id, child_id) VALUES (%d, %d)" % (dir_id, cid)
        cursor.execute(sql)
        db.commit()
        dir_id = cid
        return dir_id
    return False


def ls(db, path: str):
    cursor = db.cursor()

    dirs = path.split('/')
    dirs[0] = 'root'
    dir_id = 1
    result = []
    
    if dirs[1] == '':
        sql = "select * from directory where parent_id = %d" % (dir_id)
        cursor.execute(sql)
        res = cursor.fetchall()
        for _ in res:
            metadata = get_metadata(db, _[2])
            result.append('name: %s  type: %s' % (metadata[1], metadata[2]))
        return '\n'.join(result)
    
    for dir in dirs[1:]:
        exist_dir = False
        sql = "select * from directory where parent_id = %d" % (dir_id)
        cursor.execute(sql)
        res = cursor.fetchall()
        for _ in res:
            #(id, pid, cid)
            metadata = get_metadata(db, _[2])
            #(id, name, type, content)
            if metadata[1] == dir and metadata[2] == 'directory':
                dir_id = metadata[0]
                
                exist_dir = True
        if not exist_dir:
            print("Not Exist this dir")
            return
    sql = "select * from directory where parent_id = %d" % (dir_id)
    cursor.execute(sql)
    res = cursor.fetchall()
    for _ in res:
        metadata = get_metadata(db, _[2])
        result.append('name: %s  type: %s' % (metadata[1], metadata[2]))
    return '\n'.join(result)


def cat(db, path: str):
    cursor = db.cursor()

    dirs = path.split('/')
    dirs[0] = 'root'
    dir_id = 1
    for dir in dirs[1:]:
        exist_dir = False
        sql = "select * from directory where parent_id = %d" % (dir_id)
        cursor.execute(sql)
        res = cursor.fetchall()
        for _ in res:
            #(id, pid, cid)
            metadata = get_metadata(db, _[2])
            #(id, name, type, content)
            if metadata[1] == dir and metadata[2] == 'directory':
                dir_id = metadata[0]
                exist_dir = True
                break
            elif metadata[1] == dir and metadata[2] == 'partition' and dir == dirs[-1]:
                dir_id = metadata[0]
                exist_dir = True
                break
                
        if not exist_dir:
            print("Not Exist this dir")
            return False
        
    partitions = getPartitionLocations(db, dirs[-1])
    
    df = pd.DataFrame()
    for i in range(len(partitions)):
        data = readPartition(db, partitions, i)
        if df.empty:
            df = data
        else:
            df = pd.concat([df, data])
    return df.to_string()

def rm(db, path: str):
    cursor = db.cursor()

    dirs = path.split('/')
    dirs[0] = 'root'
    dir_id = 1
    for dir in dirs[1:]:
        exist_dir = False
        sql = "select * from directory where parent_id = %d" % (dir_id)
        cursor.execute(sql)
        res = cursor.fetchall()
        for _ in res:
            #(id, pid, cid)
            metadata = get_metadata(db, _[2])
            #(id, name, type, content)
            if metadata[1] == dirs[-1] and metadata[2] == 'partition':
                dir_id = metadata[0]
                exist_dir = True
            if metadata[1] == dir and metadata[2] == 'directory':
                dir_id = metadata[0]
                exist_dir = True
                break
        if not exist_dir:
            print("Not Exist this dir")
            return False

    sql = "select * from directory where parent_id = %d" % (dir_id)
    cursor.execute(sql)
    res = cursor.fetchall()
    if len(res) >= 1:
        print("Delete Error: Directory contains files/directories")
        return False

    sql = "delete from directory where child_id = %d" % (dir_id)
    cursor.execute(sql)
    db.commit()
    
    cursor.execute('delete from metadata where id = %d' % (dir_id))
    db.commit()
    return True


def put(db, path: str, filename: str, k: int = 10):
    cursor = db.cursor()

    dirs = path.split('/')
    dirs[0] = 'root'
    dir_id = 1
    for dir in dirs[1:]:
        exist_dir = False
        sql = "select * from directory where parent_id = %d" % (dir_id)
        cursor.execute(sql)
        res = cursor.fetchall()
        for _ in res:
            #(id, pid, cid)
            metadata = get_metadata(db, _[2])
            
            #(id, name, type, content)
            if metadata[1] == dir and metadata[2] == 'directory':
                dir_id = metadata[0]
                exist_dir = True
                break
            
        if not exist_dir:
            print("Not Exist this dir")
            return False
    
    sql = "select * from directory where parent_id = %d" % (dir_id)
    cursor.execute(sql)
    res = cursor.fetchall()

    for _ in res:
        metadata = get_metadata(db, _[2])

        if metadata[1] == filename:
            return False

    sql = "INSERT INTO metadata(name, type) VALUES (%s, %s)"
    val = (filename, 'partition')
    cursor.execute(sql, val)
    db.commit()
    
    cid = get_max_id(db, 'metadata')
    sql = "INSERT INTO directory(parent_id, child_id) VALUES (%d, %d)" % (dir_id, cid)
    cursor.execute(sql)
    db.commit()
    
    fid = cid
    from sqlalchemy import create_engine
    import math
    engine = create_engine(
        'mysql+pymysql://admin:Dsci-551@mysql.ccptbaes8l3r.us-west-1.rds.amazonaws.com:3306/edfs?charset=utf8',
        encoding='utf-8'
    )
    csvdata = pd.read_csv('polls/'+filename)
    df_num = len(csvdata)
    every_epoch_num = math.floor((df_num/k))
    for index in range(k):
        tablename = filename[:-4] + str(index)
        if index < k-1:
            csvdata[every_epoch_num * index: every_epoch_num * (index + 1)].to_sql(
                tablename, engine, if_exists='replace', index=False, chunksize=100)
        else:
            csvdata[every_epoch_num * index:].to_sql(
                tablename, engine, if_exists='replace', index=False, chunksize=100)
        sql = "INSERT INTO partitioninfo(fid, partition_name) VALUES (%d, '%s')" % (fid, tablename)
        cursor.execute(sql)
        db.commit()
    return True


def getPartitionLocations(db, filename: str):
    cursor = db.cursor()

    sql = "SELECT id FROM metadata WHERE name = '%s'" % (filename)
    cursor.execute(sql)
    fid = cursor.fetchone()[0]
    sql = "SELECT partition_name FROM partitioninfo WHERE fid = %d" % (fid)
    cursor.execute(sql)
    partitions = cursor.fetchall()
    partitions = [p[0] for p in partitions]
    return partitions

def readPartition(db, partitons, i):
    
    cursor = db.cursor()
    sql = "SELECT * FROM `%s`" % partitons[i]
    cursor.execute(sql)
    data = cursor.fetchall()
    columnDes = cursor.description
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)
    return df


# In[158]:


def query(db, queries):
    cursor = db.cursor()
    res = pd.DataFrame()
    
    for sql in queries:
        
        cursor.execute(sql)
        data = cursor.fetchall()
        columnDes = cursor.description
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        df = pd.DataFrame([list(i) for i in data], columns=columnNames)
        if res.empty:
            res = data
        else:
            res = pd.concat([df, data])
    return res.to_dict(orient="records")


# In[19]:


# Group by
#res: [{key1: [val0, val1, val2...]}, {key2: [val0, val1, val2...]}...]
def map(data, key, val):
    res = dict()
    
    for item in data:
        if item[key] not in res:
            res[item[key]] = []
        res[item[key]].append(item[val])
    return res


# In[20]:


#data: [{key1: [val0, val1, val2...]}, {key2: [val0, val1, val2...]}...]
#cmd: count, avg, max, min, sum
#condition: tuple(cmd, lt/gt/gte/lte/eq, val)
def reduce(data, cmd, condition=None):
    res = dict()
    
    for k in data:
        item = data[k]
        result = 0
        if cmd == 'count':
            result = len(item)
        elif cmd == 'avg':
            result = sum(item) / len(item)
        elif c == 'max':
            result = max(item)
        elif c == 'min':
            result = min(item)
        elif c == 'sum':
            result = sum(item)
            
        if condition is not None:
            c = condition[0]
            con_tmp = 0
            if c == 'count':
                con_tmp = len(item)
            elif c == 'avg':
                con_tmp = sum(item) / len(item)
            elif c == 'max':
                con_tmp = max(item)
            elif c == 'min':
                con_tmp = min(item)
            elif c == 'sum':
                con_tmp = sum(item)
            c = condition[1]
            v = condition[2]
            if c == 'lt' and con_tmp < v:
                res[k] = result
            elif c == 'gt' and con_tmp > v:
                res[k] = result
            elif c == 'lte' and con_tmp <= v:
                res[k] = result
            elif c == 'gte' and con_tmp >= v:
                res[k] = result
            elif c == 'eq' and con_tmp == v:
                res[k] = result
        else:
            res[k] = result
    return res

"""
db = pymysql.connect(host=hostname,
                     user=uname,
                     password=pwd,
                     database=dbname,
                     port=3306)
cursor = db.cursor()


init_database()
"""

