#! /usr/local/bin/python3

import common
import urllib.request
import json
import threading
from sys import argv


plateList = None
currentIndex = 0
lock = None
newPlates = []

def next_plate_id():

    global plateList
    global currentIndex
    global lock

    lock.acquire()
    print("current index ", currentIndex)
    if len(plateList) > currentIndex:
        plateId = plateList[currentIndex]
        currentIndex = currentIndex + 1
        lock.release()
        return plateId[0]
    lock.release()
    return None
        
    
def scrap_info(conn,symbol):

    query = "http://kanpanbao.com/kql?q={\"fun\":[\"kv.mget\",{\"keys\":[\"rank_%s_up\"]}]}" % symbol

    f = urllib.request.urlopen(query)
    result = f.read().decode('utf8')
    try:
        j = json.loads(result)
        if j["code"] == 0:
            fun = j["data"]["fun"]
            for (k,v) in fun.items() :
                if isinstance(v,list):
                    for stock_info in v:
                        store(conn,symbol,stock_info)
    except IOError as e:
        print("request error : ",e)
        


def store(conn,plate_id , info):

    stock_id = info["symbol"]
    
    sql = "select * from plate_info where plate_symbol = '%s' and symbol = '%s'; " % (plate_id,stock_id)
    dbcur = conn.cursor()
    count = dbcur.execute(sql)
    if count > 0:
        return 
    sql = "insert into plate_info(plate_symbol,symbol) values('%s','%s');" %(plate_id,stock_id)
    dbcur.execute(sql)
    conn.commit()
    
def sync_plate_data(args,argv):

    conn = args
    while True:
        plateId = next_plate_id()
        if plateId == None:
            break
        scrap_info(conn,plateId)
        
    
def fetch_plates_info():

    global plateList
    global lock

    lock = threading.Lock()

    dbconnection = common.initDatabase()
    dbcur = dbconnection.cursor()

    sql = "select symbol from list where symbol like 'bk%'"

    count = dbcur.execute(sql)

    if count == 0:
        print("no plate >>>")
        exit(-1)


    plateList = dbcur.fetchall()
    
    for i in range (1):

        conn = common.initDatabase()
        t = threading.Thread(target = sync_plate_data , args=(conn,""))
        t.start()

    dbconnection.close()

def check_has_plate(conn , plate_id):

    sql = "select symbol from list where symbol = '%s';" % plate_id

    dbcur = conn.cursor()
    count =dbcur.execute(sql)
    if count > 0 :
        return True

    return False

def try_store_plate(conn , info):

    if "symbol" not in info:
        print("error info : \n",info)
        return


    name = info["name"]
    if name == "测试板块":
        print("find test")
        return
    plate_id = info["symbol"]
    if check_has_plate(conn,plate_id):
        print("already has :",info["name"])
        return

    sql = "insert into list (symbol , type , name , template) values('%s','BK','%s','%s'); " %(plate_id , info["name"],info["tpl"])
    sql = sql.encode('utf-8')
    
    dbcur = conn.cursor()
    dbcur.execute(sql)
    conn.commit()
    
    newPlates.append(info)

    
def update_hy_plate():

    conn = common.initDatabase()

    types = ["rank_cn_hy_up","rank_cn_gn_up"]
    
    '''
     获取行业板块列表
    '''
    for t in types:
        query = "http://foo.kanpanbao.com/kql?q={\"list\":[\"kv.mget\",{\"keys\":[\"%s\"]}]}" % t
        f = urllib.request.urlopen(query)
        result = f.read().decode('utf8')
        try:
            j = json.loads(result)
            data = j["data"]
            plate_list = data["list"]
            plates = plate_list[t]
            for info in plates:
                try_store_plate(conn,info)
        except IOError as e:
            print("find except : ",e)

    conn.close()

    #print("new plates is: ",newPlates)
    #return 
    if len(newPlates) > 0 :
        fetch_plates_info()
    

if __name__ == '__main__':

    if len(argv) == 1:
        fetch_plates_info()
    else:
        update_hy_plate()