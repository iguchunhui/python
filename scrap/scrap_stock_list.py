#! /usr/local/bin/python3


import pprint
from sys import argv
import urllib.request
import json
import pymysql
import time
import threading


dbcur = None
dbconnection = None
nextStockIndex = None

def initDatabase():

    global dbcur
    global dbconnection
    
    try:
        conn = pymysql.connect(host='localhost',user='root',password='',db='stock',port=3306,charset='utf8')
        dbcur = conn.cursor()
        dbconnection = conn
    except IOError as e:
        print("mysql error : ",e)
    

def scrap(index):

    global nextStockIndex
    
    if index > 66699:
        return False

    nextStockIndex = None
        
    strIndex = "%05d" % index
    
    print("str index is: %s" % strIndex)
#    print(strIndex)

    query = "http://kanpanbao.com/kql?_app=android&_ver=1.2.0&_osv=5.0.1&q={\"symbols\":[\"symbol.s\",{\"mkts\":[],\"types\":[],\"q\":\"%s\"}]}" % strIndex
    print("query is: " , query)
    f = urllib.request.urlopen(query)
    result = f.read().decode('utf-8')

    try:
        j = json.loads(result)

        #print("\n\njson is:\n",j)

        if j["code"] == 0 :
            storeStock(j["data"])
        else:
            print("query %s is failed " % query)
    except IOError :
        print("json error is: \n ")
    finally:
        
        nextStockIndex = index + 1
        
    return True
    
def storeStock(stockDict):

    global dbcur
    global dbconnection
    symbols = stockDict["symbols"]
    for sym in symbols:
        #print(sym)
        sql = "select * from list where symbol = '%s' ; " % sym["symbol"]
        count = dbcur.execute(sql)
        if count > 0 :
            continue
        sql = "insert into list(symbol,type,name,template) values('%s','%s','%s','%s');" % (sym["symbol"],sym["type"],sym["name"],sym['tpl'])

        sql = sql.encode('utf-8')
        print("sql is: ",sql)
        dbcur.execute(sql)
        
    dbconnection.commit()

def dbtest():

    global dbcur
    global dbconnection
    sql = "insert into list(symbol,type,name,template) values('112','idx','xx','index');"
    dbcur.execute(sql)
    dbconnection.commit()

def checkCanScrapNext():

    index = 66700 #976
    while True:
        if not  scrap(index) :
            break
        index = index + 1
            
def test():

    
    initDatabase()
    checkCanScrapNext()
    
    #scrap(0)

    #dbtest()

if __name__ == '__main__':

    test()