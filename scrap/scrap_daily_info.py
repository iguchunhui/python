#! /usr/bin/python3
# /usr/local/bin/python3

import urllib.request
import time
import threading
import pymysql
import json

import common

stockList = None
currentIndex = 2165
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
        
                
'''            
                
def store_mline_data(conn,symbol , data):

    date = data["date"]
    syms = data["list"]
    for d in syms :
        t = d[0] # time
        p = d[1] #price 
        v = d[2] #volume
        sql = "select * from mline where symbol = '%s' and date = '%s' and time = '%s'" % (symbol , date , t)
        try:
            dbcur = conn.cursor()
            count = dbcur.execute(sql)
            if count == 0 :
                #print("will insert new data")
                sql = "insert into mline(symbol,date,time,price,volume) values('%s','%s','%s',%f,%f)" %(symbol,date,t,p,v)
            #print("sql is: ",sql)
                dbcur.execute(sql)
            #else:
            #    print("alread has this data will return")
                
        except IOError as e:
            print("insert mline error",e)
    conn.commit()
    
def store_kline_data(conn,symbol , klist , fq , ktype):

    dbcur = conn.cursor()

    for item in klist:
        date =  common.secureValue(item,0)
        last =  common.secureValue(item,1)
        kopen = common.secureValue(item,2)
        high = common.secureValue(item,3)
        close = common.secureValue(item,4)
        low = common.secureValue(item,5)
        volume = common.secureValue(item,6)
        amount = common.secureValue(item,7)
        percent = common.secureValue(item,8)
        ma5 = common.secureValue(item,9)
        ma10 = common.secureValue(item,10)
        ma20 = common.secureValue(item,11)

        
        sql = "select * from kline where symbol = '%s' and fq = '%s' and date ='%s' and type = '%s' ; " % (symbol , fq , date , ktype)
        sql = sql.encode('utf-8')
        #print("sql is: " , sql)
        count = dbcur.execute(sql)

        if count > 0 :
            continue
        sql = "insert into kline(symbol , date , last , open , high , close , low , volume , amount , percent , ma5 , ma10 , ma20 , fq , type) values('%s','%s',%f , %f , %f , %f , %f , %f , %f , %f , %f , %f , %f , '%s' , '%s') ; " % (symbol , date , last , kopen , high , close , low , volume , amount , percent , ma5 , ma10 ,ma20 , fq , ktype)

        #print("sql is: ",sql)
        sql = sql.encode('utf-8')
        #print("insert sql: " , sql)
        dbcur.execute(sql)
        
    conn.commit()

def store_quote_data(conn,symbol , data):

    dbcur = conn.cursor()
    datetime = data["time"]
    datetime = common.format_time(datetime)
    
    sql = "select * from quote where symbol = '%s' and time = '%s'; " % (symbol , datetime)
    count = dbcur.execute(sql)
    if count > 0 :
        return 
    
    timestamp = common.secureDictValue(data,"mtime",0)
    high = common.secureDictValue(data,"high",0)
    low = common.secureDictValue(data,"low",0)
    preclose = common.secureDictValue(data,"preclose",0)
    open_price = common.secureDictValue(data,"open",0)
    last = common.secureDictValue(data,"last",0)
    vol = common.secureDictValue(data,"vol",0)
    amt = common.secureDictValue(data,"amt",0)
    qrr = common.secureDictValue(data,"qrr",0)
    diff = common.secureDictValue(data,"diff",0)
    change = common.secureDictValue(data,"change",0)
    amp = common.secureDictValue(data,"amp",0)
    trade = common.secureDictValue(data,"tr",0)
    pe = common.secureDictValue(data,"pe",0)
    pb = common.secureDictValue(data,"pb",0)
    status = common.secureDictValue(data,"","L")
    mc  = common.secureDictValue(data,"mc",0)
    cmc  = common.secureDictValue(data,"cmc",0)
    tso  = common.secureDictValue(data,"tso",0)
    shares  = common.secureDictValue(data,"shares",0)
    b1p = common.secureDictValue(data,"b1p",0)
    b1v = common.secureDictValue(data,"b1v",0)
    b2p = common.secureDictValue(data,"b2p",0)
    b2v = common.secureDictValue(data,"b2v",0)
    b3p = common.secureDictValue(data,"b3p",0)
    b3v = common.secureDictValue(data,"b3v",0)
    b4p = common.secureDictValue(data,"b4p",0)
    b4v = common.secureDictValue(data,"b4v",0)
    b5p = common.secureDictValue(data,"b5p",0)
    b5v = common.secureDictValue(data,"b5v",0)
    s1p = common.secureDictValue(data,"s1p",0)
    s1v = common.secureDictValue(data,"s1v",0)
    s2p = common.secureDictValue(data,"s2p",0)
    s2v = common.secureDictValue(data,"s2v",0)
    s3p = common.secureDictValue(data,"s3p",0)
    s3v = common.secureDictValue(data,"s3v",0)
    s4p = common.secureDictValue(data,"s4p",0)
    s4v = common.secureDictValue(data,"s4v",0)
    s5p = common.secureDictValue(data,"s5p",0)
    s5v = common.secureDictValue(data,"s5v",0)

    sql = "insert into quote(symbol , time , high , low , preclose , open , last , volume , amount , qrr , timestamp , diff , price_change , amp , trade , pe , pb , status , mc , cmc , tso , share , bid_1_price , bid_1_volume , bid_2_price , bid_2_volume,bid_3_price,bid_3_volume , bid_4_price , bid_4_volume , bid_5_price , bid_5_volume , sell_1_price , sell_1_volume , sell_2_price , sell_2_volume , sell_3_price , sell_3_volume , sell_4_price , sell_4_volume , sell_5_price , sell_5_volume) values('%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,'%s',%f,%f,%f,%f, %f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f )" %(symbol,datetime,high,low,preclose,open_price,last,vol,amt,qrr,timestamp,diff,change,amp,trade,pe,pb,status,mc,cmc,tso,shares,b1p,b1v,b2p,b2v,b3p,b3v,b4p,b4v,b5p,b5v,s1p,s1v,s2p,s2v,s3p,s3v,s4p,s4v,s5p,s5v)


    dbcur.execute(sql)

    conn.commit()
'''        
    
def sync_today_data(args,argv):

    conn = args
    while True:
        stockId = next_stock_id()
        if stockId == None:
            break
        scrap_info(conn,stockId)

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
        
