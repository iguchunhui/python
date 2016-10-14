#! /usr/local/bin/python3
import pymysql
import time




def initDatabase():

    try:
        conn = pymysql.connect(host='localhost',user='root',password='',db='stock')
        return conn
    except IOError as e:
        print("mysql error : ",e)
        exit(-1)

def init_scrap_info():

    conn = initDatabase()
    dbcur = conn.cursor()

    sql = "select symbol from list order by symbol;"

    count = dbcur.execute(sql)
    if count == 0:
        print("no stock may be mysql error")
        exit(-1)
    stockList = dbcur.fetchall()

    for stock_tuple in stockList:

        #print(stock_tuple[0])
        sql = "insert into scrap_mline values ('%s',0 ); " % stock_tuple[0]
        dbcur.execute(sql)
        
        sql = "insert into scrap_kline values ('%s',0 ); " % stock_tuple[0]
        dbcur.execute(sql)

    conn.commit()
        
    conn.close()

def reset_scrap_info():

    conn = initDatabase()
    dbcur = conn.cursor()

    sql = "update scrap_kline set scraped = 0;"
    dbcur.execute(sql)
    sql = "update scrap_mline set scraped = 0;"
    dbcur.execute(sql)
    
    conn.commit()

    conn.close()

if __name__ == '__main__':
    
    
    #init_scrap_info()
    reset_scrap_info()
    
    
    