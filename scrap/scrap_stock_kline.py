#! /usr/local/bin/python3

from sys import argv
import urllib.request
import json
import pymysql
import time
import threading


dbcur = None
dbconnection = None
nextStockIndex = None
stockList = None
currentIndex = 2726
lock = None

def initDatabase():
    
    try:
        conn = pymysql.connect(host='localhost',user='root',password='',db='stock',port=3306,charset='utf8')
#        dbcur = conn.cursor()

        return conn
    except IOError as e:
        print("mysql error : ",e)


def scrap(conn, symbol,kline_type , fq_type):
    
    offset = 0
    limit = 500
    while True:

        query = "http://kanpanbao.com/kql?q={\"dline\":[\"kline.%s\",{\"symbol\":\"%s\",\"type\":\"%s\",\"mtime\":0 , \"offset\":%d,\"limit\":%d}]}" % (kline_type , symbol , fq_type , offset ,limit)

        #print(query)

        f = urllib.request.urlopen(query)
        result = f.read().decode('utf8')

        try:
            j = json.loads(result)
            #print("json is: \n",j)
            if j["code"] == 0 :
                klist = j["data"]["dline"]["list"]
                if len(klist) == 0 :
                    #print("get kline data for %s done"% symbol)
                    return
                else:
                    storeKlineData(conn,symbol , klist , fq_type , kline_type)
            else:
                print("query %s is failed " % query)
        except IOError:
            print("json error is: \n",result)
        finally:
            offset += limit
            #print("offset is: " , offset)

def secureValue(item,index):

    if item[index] == None :
        return 0
    return item[index]
        
def storeKlineData(conn,symbol , klist , fq , ktype):

    dbcur = conn.cursor()
    dbconnection = conn
    
    for item in klist:
        date =  secureValue(item,0)
        last =  secureValue(item,1)
        kopen = secureValue(item,2)
        high = secureValue(item,3)
        close = secureValue(item,4)
        low = secureValue(item,5)
        volume = secureValue(item,6)
        amount = secureValue(item,7)
        percent = secureValue(item,8)
        ma5 = secureValue(item,9)
        ma10 = secureValue(item,10)
        ma20 = secureValue(item,11)
            
        sql = "select * from kline where symbol = '%s' and fq = '%s' and date = '%s' and type = '%s' ; " % (symbol , fq , date , ktype)
        sql = sql.encode('utf-8')
        #print("sql is: " , sql)
        count = dbcur.execute(sql)
        
        if count > 0 :
            continue
        sql = "insert into kline(symbol , date , last , open , high , close , low , volume , amount , percent , ma5 , ma10 , ma20 , fq , type) values('%s','%s',%f , %f , %f , %f , %f , %f , %f , %f , %f , %f , %f , '%s' , '%s') ; " % (symbol , date , last , kopen , high , close , low , volume , amount , percent , ma5 , ma10 ,ma20 , fq , ktype)

        sql = sql.encode('utf-8')
        #print("insert sql: " , sql)
        dbcur.execute(sql)

    dbconnection.commit()


def scrap_stock(args,argv):

    conn = args
    
    kline_types = ["day","week","month"]
    fq_types = ["qfq","hfq","bfq"]
    while True:
        stockId = next_stock_id()
        if stockId == None:
            break
        for ktype in kline_types:
            for fqtype in fq_types:
                scrap(conn,stockId,ktype,fqtype)
                #print("scrap %s  %s %s " %(stockId , ktype , fqtype))
                pass

def next_stock_id():

    global stockList
    global currentIndex
    global lock
    
    lock.acquire()
    print("current Index is: ",currentIndex)
    if len(stockList) > currentIndex :
        stockId = stockList[currentIndex]
        currentIndex = currentIndex + 1
        lock.release()
        print("load stock: ",stockId[0])
        return stockId[0]
    lock.release()
    return None
        
    
def fetch_stock():

    global dbcur
    global dbconnection
    global stockList
    global lock

    lock = threading.Lock()
    
    
    dbconnection = initDatabase()
    dbcur = dbconnection.cursor()
    
    sql = "select symbol from list order by symbol;"

    count = dbcur.execute(sql)
    if count == 0:
        print("no stock >>>")
        return
    stockList = dbcur.fetchall()
    #print("all stock is:\n",stockList)
    for i in range(3):
        conn = initDatabase()
        t1 = threading.Thread(target = scrap_stock,args = (conn,"") )
        t1.start()

    
def test():

    initDatabase()
    fetch_stock()


if __name__ == '__main__':

    test()