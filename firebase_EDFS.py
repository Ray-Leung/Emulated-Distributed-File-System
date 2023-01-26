#!/usr/bin/env python
# coding: utf-8

import firebase_admin
from firebase_admin import credentials
import pandas as pd
from firebase_admin import db


def init_edfs():
    ref = db.reference("/metadata")
    if ref.get() is None:
        ref.child('-1').set({"lastInodeId": 0, "numInodes": 1})
        ref.child('0').set({"name" : "", "type" : "DIRECTORY", "child" : []})


def find_parent(path):
    ref = db.reference('/metadata')
    folders = path.split('/')
    if folders[0] != '':
        return False
    
    idx = 0
    
    for i in range(1, len(folders)):
        inode = ref.child(str(idx)).get()
        if inode is None:
            return False
        if 'child' not in inode and i < len(folders) - 1:
            return False
        if 'child' not in inode and i == len(folders) - 1:
            return idx
        
        child = inode['child']
        for child_id in child:
            child_node = ref.child(str(child_id)).get()
            if child_node['name'] == folders[i]:
                if i == len(folders) - 1:
                    return False
                idx = child_id
                break
    return idx


def find_cur(path):
    ref = db.reference('/metadata')
    folders = path.split('/')
    if folders[0] != '':
        return False
    
    idx = 0
    
    for i in range(1, len(folders)):
        inode = ref.child(str(idx)).get()
        if inode is None and i < len(folders) - 1:
            return False
        if 'child' not in inode and i < len(folders) - 1:
            return False
        if 'child' not in inode and i == len(folders) - 1:
            return idx
        
        child = inode['child']
        for child_id in child:
            child_node = ref.child(str(child_id)).get()
            if child_node['name'] == folders[i]:
                idx = child_id
                break
    return idx


def mkdir(path):
    ref = db.reference('/metadata')
    folders = path.split('/')
    idx = find_parent(path)
    
    if (idx is False):
        return False
    
    x = ref.child('-1').get()
    new_id = x['lastInodeId'] + 1
    new_num = x['numInodes'] + 1
    ref.child(str(-1)).update({'lastInodeId' : new_id, 'numInodes' : new_num})
    
    parent = ref.child(str(idx)).get()
    if 'child' not in parent:
        children = []
    else:
        children = parent['child']
    children.append(new_id)
    ref.child(str(idx)).update({"child": children})
    dirct = {"name" : folders[-1], "type" : "DIRECTORY", "child" : []}
    x = ref.child(str(new_id)).set(dirct)
    return True


def ls(path):
    lst = []
    ref = db.reference('/metadata')

    idx = find_cur(path)
    
    if (idx is False):
        return False
                
    inode = ref.child(str(idx)).get()
    child = inode['child']
    
    for c_id in child:
        c = ref.child(str(c_id)).get()
        lst.append(c['name'])

    return '  '.join(lst)


def put(path, f_name, k=10):
    if k < 10:
        print('Please increase partition # to at least 10')
        return False
    
    ref = db.reference('/metadata')
    folders = path.split('/')
    
    idx = find_cur(path)

    if (idx is False):
        return False
        
    x = ref.child('-1').get()
    new_id = x['lastInodeId'] + 1
    new_num = x['numInodes'] + 1
    ref.child(str(-1)).update({'lastInodeId' : new_id, 'numInodes' : new_num})
    
    parent = ref.child(str(idx)).get()
    if 'child' not in parent:
        children = []
    else:
        children = parent['child']
        for child_id in children:
            child = ref.child(str(child_id)).get()
            if child['name'] == f_name:
                return False
    children.append(new_id)
    ref.child(str(idx)).update({"child": children})
    collection = ['/' + str(new_id) + "_" + "p" + str(i) for i in range(k)]
    
    #read file here
    if f_name != 'Colleges_and_Universities.json':
        print(1)
        return False
    
    dirct = {"name" : f_name, "type" : "FILE", "replication" : k, "collection" : collection}
    x = ref.child(str(new_id)).set(dirct)
    df = pd.read_csv("polls/"+f_name[:-5] + '.csv')
    df.index = df.index.astype(str)
    N = df.shape[0]
    step = N // k
    e = step
    s = 0
    for coll in collection:
        ref = db.reference(coll)
        data = df[s : e].to_dict(orient='index')
        ref.set(data)
        s = e
        e += step
    return True


def getPartitionLocations(file):
    ref = db.reference('/metadata')
    x = ref.get()

    for k in x:
        if k == '-1':
            continue
        if x[k]['name'] == file:
            return x[k]['collection']
        
    
    return None


def readPartition(file, p_idx, collections):
    #collections = getPartitionLocations(file, db)
    
    col = db.reference(collections[p_idx])
    return col


def cat(path):
    ref = db.reference('/metadata')
    folders = path.split('/')
    res = []
    idx = find_cur(path)
    
    if (idx is False):
        return False
    
    parent = ref.child(str(idx)).get()
    f_name = parent['name']
    k = parent['replication']
    partitions = getPartitionLocations(f_name)
    
    for i in range(2):
        f_col = readPartition(f_name, i, partitions).get()
        
        for data in f_col:
            res.append(str(data))
            print(data)
    return '\n'.join(res)


def rm(path):
    ref = db.reference('/metadata')
    folders = path.split('/')
    
    p_id = -1
    cur_id = 0
    
    if folders[0] != '':
        return None
    
    for i in range(1, len(folders)):
        inode = ref.child(str(cur_id)).get()
        child = []
        if 'child' in inode:
            child = inode['child']
        if i < len(folders) - 1 and len(child) == 0:
            print("Parent directory does not exist.")
            return False
        
        for c_id in child:
            child_node = ref.child(str(c_id)).get()
            if (child_node["name"] == folders[i]):
                p_id = cur_id
                cur_id = c_id
                break
    
    parent = ref.child(str(p_id)).get()
    current = ref.child(str(cur_id)).get()
    
    if (current['type'] == 'FILE'):
        collection = current['collection']
        
        for c in collection:
            mycol = db.reference(c)
            mycol.delete()
    
    if (current['type'] == 'DIRECTORY' and 'child' in current):
        return False
    
    children = parent["child"]
    children.remove(cur_id)
    ref.child(str(p_id)).update({"child": children})
    ref.child(str(cur_id)).delete()
    
    return True


def query(loc):
    partitions = getPartitionLocations(loc+'.json')
    k = len(partitions)
    res = []
    
    for i in range(k):
        f_col = readPartition(loc+'.json', i, partitions)            
        data = f_col.get()
        for it in data:
            res.append(it)
    return res



def query(fiedls, loc):
    partitions = getPartitionLocations(loc+'.json')
    k = len(partitions)
    res = []
    
    for i in range(k):
        f_col = readPartition(loc+'.json', i, partitions)            
        data = f_col.get()
        for it in data:
            res.append({f : it[f] for f in fiedls})
    return res


#conditions: [(key, cmd, val)]
#cmd: lt/gt/lte/gte/eq

def map(data, key, val, conditions):
    res = dict()
    
    for d in data:
        if d[key] not in res:
            res[d[key]] = []
            
        for con in conditions:
            cmd = con[1]
            if cmd == 'lt':
                if d[con[0]] < con[2]:
                    res[d[key]].append(d[val])
            elif cmd == 'gt':
                if d[con[0]] > con[2]:
                    res[d[key]].append(d[val])
                    
            elif cmd == 'lte':
                if d[con[0]] <= con[2]:
                    res[d[key]].append(d[val])
                    
            elif cmd == 'gte':
                if d[con[0]] >= con[2]:
                    res[d[key]].append(d[val])
                    
            elif cmd == 'eq':
                if d[con[0]] == con[2]:
                    res[d[key]].append(d[val])
    return res


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
firebase_admin.delete_app(default_app)
databaseURL = 'https://dsci551-350cc-default-rtdb.firebaseio.com/'
cred = credentials.Certificate('polls/firebase_key.json')
default_app = firebase_admin.initialize_app(cred, {'databaseURL':databaseURL})
init_edfs()
"""