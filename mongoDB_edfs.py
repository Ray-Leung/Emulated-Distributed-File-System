#!/usr/bin/env python
# coding: utf-8

import pymongo
import pandas as pd


def connect_mongoDB():
    ssh_address = "ec2-54-67-123-50.us-west-1.compute.amazonaws.com"
    ssh_port = 27017
    user = "dbAdmin"
    pwd = "Dsci-551"

    try:
        myclient = pymongo.MongoClient("mongodb://"+ user + ":" + pwd+ "@" +ssh_address+":27017/?authMechanism=DEFAULT")
        
        return myclient
        #myclient.close()

    except Exception as e:
        print("MongoDB connection failure: Please check the connection details")
        print(e)


def init_edfs(mydb):
    mycol = mydb["metadata"]

    if not (mycol.find_one({"_id" : -1})):
        mydict = {"_id" : -1, "lastInodeId": 0, "numInodes": 1 }
        x = mycol.insert_one(mydict)

        mydict = {"ID" : 0, "name" : "", "type" : "DIRECTORY", "child" : []}
        if not (mycol.find_one({"ID" : 0})):
            x = mycol.insert_one(mydict)


def find_parent(path, db):
    col = db['metadata']
    folders = path.split('/')
    if folders[0] != '':
        return False
    
    idx = 0
    
    for i in range(1, len(folders)):
        inode = col.find_one({"ID" : idx})
        child = inode['child']
        if i < len(folders) - 1 and len(child) == 0:
            print("Parent directory does not exist.")
            return False
        
        for c_id in child:
            child_node = col.find_one({"ID" : c_id})
            if (child_node["name"] == folders[i]):
                if (i == len(folders) - 1):
                    return False
                idx = c_id
                break
    return idx


def find_cur(path, db):
    col = db['metadata']
    folders = path.split('/')
    idx = 0
    if folders[0] != '':
        return None
    
    for i in range(1, len(folders)):
        inode = col.find_one({"ID" : idx})
        child = inode['child']
        if i < len(folders) - 1 and len(child) == 0:
            print("Parent directory does not exist.")
            return False
        
        for c_id in child:
            child_node = col.find_one({"ID" : c_id})
            if (child_node["name"] == folders[i]):
                idx = c_id
                break
    return idx


def mkdir(path, db):
    col = db['metadata']
    
    folders = path.split('/')
    
    idx = find_parent(path, db)
    
    if (idx is False):
        return False
    
    x = col.find_one({"_id" : -1})
    new_id = x['lastInodeId'] + 1
    new_num = x['numInodes'] + 1
    col.update_one({"_id" : -1}, {"$set" : {"lastInodeId": new_id, "numInodes": new_num}})
    
    parent = col.find_one({"ID" : idx})
    children = parent['child']
    children.append(new_id)
    col.update_one({"ID" : idx}, {"$set" : {"child": children}})
    dirct = {"ID" : new_id, "name" : folders[-1], "type" : "DIRECTORY", "child" : []}
    x = col.insert_one(dirct)
    return True


def ls(path, db):
    lst = []
    col = db['metadata']

    idx = find_cur(path, db)
    
    if (idx is False):
        return False
                
    inode = col.find_one({"ID" : idx})
    child = inode['child']
    
    for c_id in child:
        c = col.find_one({"ID" : c_id})
        lst.append(c['name'])

    return ',  '.join(lst)


def put(db, path, f_name, k=10):
    if k < 10:
        print('Please increase partition # to at least 10')
        return False
    
    col = db['metadata']
    folders = path.split('/')
    
    idx = find_cur(path, db)

    if (idx is False):
        return False
        
    x = col.find_one({"_id" : -1})
    new_id = x['lastInodeId'] + 1
    new_num = x['numInodes'] + 1
    col.update_one({"_id" : -1}, {"$set" : {"lastInodeId": new_id, "numInodes": new_num}})
    
    parent = col.find_one({"ID" : idx})
    children = parent['child']
    
    for child_id in children:
        child = col.find_one({"ID" : child_id})
        if child['name'] == f_name:
            return False
    children.append(new_id)
    col.update_one({"ID" : idx}, {"$set" : {"child": children}})
    collection = [str(new_id) + "_" + "p" + str(i) for i in range(k)]
    
    #read file here
    if f_name != 'power.json' and f_name != 'restaurant.json':
        print(1)
        return False
    
    dirct = {"ID" : new_id, "name" : f_name, "type" : "FILE", "replication" : k, "collection" : collection}
    x = col.insert_one(dirct)
    df = pd.read_csv("polls/" + f_name[:-5] + '.csv', index_col=0)
    df.index = df.index.astype(str)
    N = df.shape[0]
    step = N // k
    e = step
    s = 0
    for coll in collection:
        my_col = db[coll]
        data = df[s : e].to_dict('records')
        my_col.insert_many(data)
        s = e
        e += step
    return True


def getPartitionLocations(file, db):
    col = db['metadata']
    
    x = col.find_one({"name" : file})
    
    return x["collection"]


def readPartition(file, p_idx, collections, db):
    #collections = getPartitionLocations(file, db)
    
    col = db[collections[p_idx]]
    return col


def cat(path, db):
    col = db['metadata']
    folders = path.split('/')
    
    idx = find_cur(path, db)
    res = []
    if (idx is False):
        return False
    
    parent = col.find_one({"ID" : idx})
    f_name = parent['name']
    k = parent['replication']
    partitions = getPartitionLocations(f_name, db)
    
    for i in range(k):
        f_col = readPartition(f_name, i, partitions, db)
        data = f_col.find(0)[0]
        data.pop('_id')
        res.append(str(data))
    return '\n'.join(res)


def rm(path, db):
    col = db['metadata']
    folders = path.split('/')
    
    p_id = -1
    cur_id = 0
    
    if folders[0] != '':
        return None
    
    for i in range(1, len(folders)):
        inode = col.find_one({"ID" : cur_id})
        child = inode['child']
        if i < len(folders) - 1 and len(child) == 0:
            print("Parent directory does not exist.")
            return False
        
        for c_id in child:
            child_node = col.find_one({"ID" : c_id})
            if (child_node["name"] == folders[i]):
                p_id = cur_id
                cur_id = c_id
                break
    
    parent = col.find_one({"ID" : p_id})
    current = col.find_one({"ID" : cur_id})
    
    if (current['type'] == 'FILE'):
        collection = current['collection']
        
        for c in collection:
            mycol = db[c]
            mycol.drop()
    
    if (current['type'] == 'DIRECTORY' and len(current['child']) > 0):
        return False
    
    children = parent["child"]
    children.remove(cur_id)
    col.update_one({"ID" : p_id}, {"$set" : {"child": children}})
    col.delete_one({"ID" : cur_id})
    
    return True

# fields:
# loc: dataset name
# conditions: searching conditions
# 
def query(fields, loc, conditions, mydb):
    partitions = getPartitionLocations(loc+'.json', mydb)
    k = len(partitions)
    res = []
    
    for i in range(k):
        f_col = readPartition(loc+'.json', i, partitions, mydb)
        data = f_col.find(conditions, fields)
        res.append('Partition_' + str(i) + ":")
        for it in data:
            res.append(it)
    return res


# Group by
#res: [{key1: [val0, val1, val2...]}, {key2: [val0, val1, val2...]}...]
def map(data, key, val):
    res = dict()
    
    for item in data:
        if type(item) is str:
            continue
        if item[key] not in res:
            res[item[key]] = []
        res[item[key]].append(item[val])
    return res


#data: [{key1: [val0, val1, val2...]}, {key2: [val0, val1, val2...]}...]
#cmd: count, avg, max, min, sum
#condition: tuple(cmd, lt/gt/gte/lte/eq, val)
def reduce(data, cmd, condition=None):
    res = []
    
    for k in data:
        tmp = dict()
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
            if c == '$lt' and con_tmp < v:
                tmp[k] = result
            elif c == '$gt' and con_tmp > v:
                tmp[k] = result
            elif c == '$lte' and con_tmp <= v:
                tmp[k] = result
            elif c == '$gte' and con_tmp >= v:
                tmp[k] = result
            elif c == '$eq' and con_tmp == v:
                tmp[k] = result
        else:
            tmp[k] = result
        if len(tmp) > 0:
            res.append(tmp)
    return res
"""
myclient = connect_mongoDB()
mydb = myclient["EDFS"]
init_edfs(mydb)
"""




