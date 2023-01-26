from django.shortcuts import render
from django.http import HttpResponse
import polls.mongoDB_edfs as mg
import polls.firebase_EDFS as fb
import polls.mysql_edfs as ms

init_flag = True
myclient = mg.connect_mongoDB()
mydb = myclient["EDFS"]
mg.init_edfs(mydb)

#fb.firebase_admin.delete_app(fb.default_app)
databaseURL = 'https://dsci551-350cc-default-rtdb.firebaseio.com/'
cred = fb.credentials.Certificate('polls/firebase_key.json')
if not fb.firebase_admin._apps:
    default_app = fb.firebase_admin.initialize_app(cred, {'databaseURL':databaseURL})

fb.init_edfs()

hostname='mysql.ccptbaes8l3r.us-west-1.rds.amazonaws.com'
dbname="edfs"
uname="admin"
pwd="Dsci-551"

db = ms.pymysql.connect(host=hostname,
                     user=uname,
                     password=pwd,
                     database=dbname,
                     port=3306)
cursor = db.cursor()


#if init_flag is True:
#    ms.init_database(db)
#    init_flag = False


def index(request):
    return HttpResponse("Hello world!")

def mongodb(request):
    
    return HttpResponse("Mongodb")

def mongodb_ls(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = mg.ls(path, mydb)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def mongodb_mkdir(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = mg.mkdir(path, mydb)
        return HttpResponse(res)
    

    return HttpResponse("Invalid Path")

def mongodb_cat(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = mg.cat(path, mydb)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def mongodb_rm(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = mg.rm(path, mydb)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def mongodb_put(request):
    path = request.GET.get('path') 
    file_name = request.GET.get('f')
    k = request.GET.get('k')
    print(path)
    if path is not None:
        if k is not None:
            res = mg.put(mydb, path, file_name, int(k))
        else:
            res = mg.put(mydb, path, file_name)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def mongodb_search(request):
    
    fs = request.GET.get('fields')
    loc = request.GET.get('loc')
    cons = request.GET.get('con')
    fs = fs.split(',')
    cons = cons.split(',')
    fields = dict()
    fields['_id'] = 0
    print()
    for f in fs:
        fields[f] = 1

    conditions = dict()

    ops = {'=' : '$eq', '>' : '$gt', '<' : '$lt', 
    '>=' : '$gte', '<=' : '$lte'}

    for c in cons:
        key = ''
        op = ''
        val = ''
        f = False

        for ch in c:
            if ch not in ops:
                if not f:
                    key += ch
                else:
                    val += ch
            elif ch in ops:
                f = True
                op += ch
        
        if val.isdigit():
            val = int(val)

        if key not in conditions:
            conditions[key] = dict()
        conditions[key][ops[op]] = val
    
    res = mg.query(fields, loc, conditions, mydb)

    return HttpResponse('\n'.join([str(r) for r in res]))

def mongodb_analyze(request):
    fs = request.GET.get('field')
    loc = request.GET.get('loc')
    cons = request.GET.get('con')
    result = request.GET.get('r')
    gcons = request.GET.get('g')

    cons = cons.split(',')
    conditions = dict()

    ops = {'=' : '$eq', '>' : '$gt', '<' : '$lt', 
    '>=' : '$gte', '<=' : '$lte'}
    if cons[0] != '':
        for c in cons:
            key = ''
            op = ''
            val = ''
            f = False

            for ch in c:
                if ch not in ops:
                    if not f:
                        key += ch
                    else:
                        val += ch
                elif ch in ops:
                    f = True
                    op += ch
            
            if val.isdigit():
                val = int(val)

            if key not in conditions:
                conditions[key] = dict()
            conditions[key][ops[op]] = val
    
    rcmd = ""
    rfield = ""

    for i in range(len(result)):
        ch = result[i]
        if ch == '(':
            rfield = result[i+1:-1]
            break
        else:
            rcmd += ch

    gcmd = ""
    gop= ""
    gval = ""
    gi = 0

    for ch in gcons:
        if gi == 0:
            if ch == '(':
                gi = 1
                continue
            gcmd += ch
        elif gi == 1:
            if ch == ')':
                gi = 2
        elif gi == 2:
            if ch not in ops:
                gi = 3
                gval = ch
                continue
            gop += ch
        elif gi == 3:
            gval += ch
    
    if gval.isdigit():
        gval = int(gval)

    gconditions = None
    if gcmd != "":
        gconditions = (gcmd, ops[gop], gval)
    print(gconditions)
    data = mg.query({"_id" : 0, fs : 1, rfield : 1}, loc, conditions, mydb)

    data = mg.map(data, fs, rfield)
    s = "Map stage:\n"
    s += '\n'.join([str(r) for r in data])
    res = mg.reduce(data, rcmd, gconditions)
    print(res)
    return HttpResponse(str(res))

def firebase(request):
    print(request)
    return HttpResponse("firebase")

def firebase_ls(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = fb.ls(path)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def firebase_mkdir(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = fb.mkdir(path)
        return HttpResponse(res)
    

    return HttpResponse("Invalid Path")

def firebase_cat(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = fb.cat(path)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def firebase_rm(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = fb.rm(path)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def firebase_put(request):
    path = request.GET.get('path') 
    file_name = request.GET.get('f')
    k = request.GET.get('k')
    print(path)
    if path is not None:
        if k is not None:
            res = fb.put(path, file_name, int(k))
        else:
            res = fb.put(path, file_name)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")


def mysql(request):
    print(request)
    return HttpResponse("mysql")


def mysql_ls(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = ms.ls(db, path)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def mysql_mkdir(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = ms.mkdir(db, path)
        return HttpResponse(res)
    

    return HttpResponse("Invalid Path")

def mysql_cat(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = ms.cat(db, path)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def mysql_rm(request):
    path = request.GET.get('path') 
    print(path)
    if path is not None:
        res = ms.rm(db, path)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")

def mysql_put(request):
    path = request.GET.get('path') 
    file_name = request.GET.get('f')
    k = request.GET.get('k')
    print(path)
    if path is not None:
        if k is not None:
            res = ms.put(db, path, file_name, int(k))
        else:
            res = ms.put(db, path, file_name)
        return HttpResponse(res)
    
    return HttpResponse("Invalid Path")