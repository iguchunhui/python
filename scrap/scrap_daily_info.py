#! /usr/local/bin/python3

import urllib.request
import time
import threading
import pymysql
import json

import common

stockList = None
currentIndex = 0
lock = None

        
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

    
def scrap_info(conn , symbol):

    '''
    #fenshi
    '''

    
    query = "http://kanpanbao.com/kql?q={ \"mline_1\":[\"mline.d1\", {\"symbol\":\"%s\", \"mtime\":0}]}" % symbol
    
    f = urllib.request.urlopen(query)
    result = f.read().decode('utf8')
    try:        
        j = json.loads(result)
        #print("json is: \n",j)
        if j["code"] == 0:
            data = j["data"]
            mline_1 = data["mline_1"]

            if not mline_1 == None:
                common.store_mline_data(conn,symbol,mline_1["data"])
                
    except IOError as e:
        print("error is: ",e)
        print("smybol is: %s \n\n\n\n",symbol)




    '''
    #k line
    '''
    
    fq_types = ["qfq","bfq","hfq"]
    for fq_type in fq_types :
        query = "http://kanpanbao.com/kql?q={\"dline\":[\"kline.day\",{\"symbol\":\"%s\",\"type\":\"%s\",\"mtime\":0 , \"offset\":0,\"limit\":1}]}" % (symbol , fq_type)
        f = urllib.request.urlopen(query)
        result = f.read().decode('utf8')
        try:
            j = json.loads(result)
            if j != None and j["code"] == 0 :
                klist = j["data"]["dline"]["list"]
                if len(klist) == 0:
                    continue
                else:
                    common.store_kline_data(conn,symbol,klist,fq_type,"day")
                    #print("store done")
            else:
                print("query %s is failed " % query)
                print("result is: \n",result)
        except IOError as e:
            print("json error is: \n",e)
            print("result is: \n",result)
        except BaseException as be:
            print("json error is :\n",be);
            print("result is: \n",result)
        
            

    #'''
    #quote
    #'''
    
    query = "http://kanpanbao.com/kql?q={\"quote\": [\"quote\", {\"mtime\": 0, \"symbols\":[\"%s\"]}]}" % symbol

    f = urllib.request.urlopen(query)
    result = f.read().decode('utf8')
    
    try:
        j = json.loads(result)
        if j["code"] == 0:
            data = j["data"]
            quote = data["quote"]["data"]
            if len(quote) > 0:
                quote_data = quote[symbol]
                if len(quote_data) > 0 :
                    common.store_quote_data(conn,symbol , quote_data)

    except IOError as e:
        print("quote error is: " , e)
        
                
    
def sync_today_data(args,argv):

    conn = args
    while True:
        stockId = next_stock_id()
        if stockId is not None:
            scrap_info(conn,stockId)
        else:
            break

    conn.close()
    
def fetch_today_info():
    
    global stockList
    global lock

    lock = threading.Lock()

    dbconnection = common.initDatabase()
    dbcur = dbconnection.cursor()

    sql = "select symbol from list order by symbol;"

    count = dbcur.execute(sql)
    if count == 0:
        print("no stock >>>")
        exit(-1)

    stockList = dbcur.fetchall()
        
    for i in range(3):
        conn = common.initDatabase()
        t = threading.Thread(target = sync_today_data,args=(conn,""))
        t.start()

if __name__ == '__main__':

    fetch_today_info()
        
