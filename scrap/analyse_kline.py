#! /usr/local/bin/python3

import common
import time
import pymysql
from datetime import datetime , date ,time
import threading


stock_list = None
lock = None
current_index = 0
now_timestamp = None
from_timestamp = None
change_list = []

def init_stock_list(conn):

    global stock_list
    sql = "select symbol from list"
    dbcur = conn.cursor()

    dbcur.execute(sql)

    stock_list = dbcur.fetchall()

    
def next_stock_id():

    global lock
    global current_index
    global stock_list
    
    
    lock.acquire()
    if current_index >= len(stock_list):
        lock.release()
        return None
        
    #for test
    #if current_index >= 100:
    #    return None
        
        
    stock_id = stock_list[current_index][0]
    current_index = current_index + 1
    lock.release()
    return stock_id

def analyse_kline_info(conn,stock_id):

    global now_timestamp
    global from_timestamp
    
    global change_list

    sql = "select * from kline where symbol = '%s' and timestamp between %.0f and %.0f and fq = 'qfq' and type = 'day' ; " % (stock_id , from_timestamp , now_timestamp)

    #print("sql is: ",sql)
    dbcur = conn.cursor()
    count = dbcur.execute(sql)
    if count == 0 :
        return

    prices = []
    result = dbcur.fetchone()

    while result:
        
        prices.append(result[2])
        result = dbcur.fetchone()
        
    now_price = prices[-1]
    prices.sort()

    #print("prices are:\n",prices)

    #print("amp is: %f %s"%((prices[-1]-prices[0])/now_price,stock_id))
    change = (prices[-1] - prices[0])/now_price
    info = {"symbol":stock_id,"change":change}
    #print("prices is: ",prices)
    #print("now price is: ",now_price)
    
    change_list.append(info)


def analyser(args,argv):

    conn = args
    while True:
        stock_id = next_stock_id()
        if stock_id == None:
            break
        analyse_kline_info(conn,stock_id)

    analyse_price()
        
def analyse_price():

    global change_list

    change_list.sort(key= lambda obj : obj.get('change'))

    for info in change_list:
        print("%s : %f "%(info["symbol"],info["change"]))
    
def analyse():

    global now_timestamp
    global from_timestamp
    global lock
    
    now_timestamp = common.now_timestamp()
    from_timestamp = common.exact_amonth_ago_ts()
    lock = threading.Lock()
    
    
    conn = common.initDatabase()
    init_stock_list(conn)
    
    
    #analyse_kline_info(conn,"sz002141")

    t = threading.Thread(target = analyser , args= (conn,""))
    t.start()
    
    #conn.close()

    print("done>>>")

def test():

    print(datetime.now())
    print(datetime.today())
    today = datetime.today()
    print("amonth ago : %.0f"%common.amonth_ago_ts())
    print(common.exact_amonth_ago_ts())

if __name__ == '__main__':

    #test()
    analyse()