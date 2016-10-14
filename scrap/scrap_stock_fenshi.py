#! /usr/local/bin/python3

import urllib.request
import json
import pymysql
import time
import threading

stockList = None
currentIndex = 0
lock = None

def initDatabase():

    try:
        conn = pymysql.connect(host='localhost',user='root',password='',db='stock')
        return conn
    except IOError as e:
        print("my sql error : ",e)

        
def next_stock_id():

    global stockList
    global currentIndex
    global lock
    
    lock.acquire()
    print("current Index is: ",currentIndex)
    if len(stockList) >= currentIndex :
        stockId = stockList[currentIndex]
        currentIndex = currentIndex + 1
        lock.release()
        print("load stock: ",stockId[0])
        return stockId[0]
    lock.release()
    return None

    
def scrap(conn , symbol):

    print("scrap ",symbol)
    

    query = "http://kanpanbao.com/kql?q={ \"mline_1\":[\"mline.d1\", {\"symbol\":\"%s\", \"mtime\":0}], \"mline_4\":[\"mline.d4\",{\"mtime\":0,\"symbol\":\"%s\"}]}" % (symbol,symbol)

    f = urllib.request.urlopen(query)
    result = f.read().decode('utf8')

    try:
        j = json.loads(result)
        #print("json is: \n",j)
        if j["code"] == 0 :
            datas = j["data"]
            mline_1 = datas["mline_1"]
            mline_4 = datas["mline_4"]

            if not mline_1 == None:
                store_mline_data(conn,symbol , mline_1["data"])
            if not mline_4 == None:
                lines = mline_4["data"]
                for d in lines :
                    store_mline_data(conn,symbol,d)
    except IOError as e:
            print("error is: ",e)
                
def store_mline_data(conn,symbol,data):

    date = data["date"]
    syms = data["list"]
    for d in syms :
        t = d[0] # time
        p = d[1] #price 
        v = d[2] #volume
        #print(" %s %f %f "%(t,p,v))
        sql = "insert into mline(symbol,date,time,price,volume) values('%s','%s','%s',%f,%f)" %(symbol,date,t,p,v)
        #print("sql is: ",sql)
        dbcur = conn.cursor()
        dbcur.execute(sql)

    conn.commit()
    
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
    
def scrap_stock(args , argv):

    conn = args

    while True:
        stockId = next_stock_id()
        if stockId == None:
            break
        scrap(conn,stockId)
    
        
def fetch_stock():

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
    
    for i in range(3):
        conn = initDatabase()
        t1 = threading.Thread(target = scrap_stock , args=(conn,""))
        t1.start()
        
if __name__ == '__main__':

    fetch_stock()
        
        
        
        
        