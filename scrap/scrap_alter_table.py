#! /usr/bin/python3

import common

def alter_kline():

    conn = common.initDatabase()
    dbcur = conn.cursor()
    sql = " select symbol , date , fq  from kline"

    count = dbcur.execute(sql)
    items = dbcur.fetchall()
    print("count is:",count)
    for item in items:
#    while True:
#        item = dbcur.fetchone()
#        if item == None:
#            break

        symbol = item[0]
        date = item[1]
        fq = item[2]
        #print("symbol %s date %s time  "%(item[0],item[1])
        timestamp = common.transfer_date_to_timestamp(date)
        sql = "update kline set timestamp = %f where symbol = '%s' and date = '%s' and fq = '%s';" %(timestamp,symbol,date,fq)
        dbcur.execute(sql)
        #print("sql is: \n%s",sql)
        
    conn.commit()


def alter_mline():

    conn = common.initDatabase()
    dbcur = conn.cursor()
    sql = " select symbol , date , time  from mline "

    count = dbcur.execute(sql)
    items = dbcur.fetchall()
    for item in items:
        symbol = item[0]
        date = item[1]
        hhmm = item[2]
        #print("symbol %s date %s time  "%(item[0],item[1])
        timestamp = common.transfer_date_and_time(date,hhmm)
        sql = "update mline set timestamp = %f where symbol = '%s' and date = '%s' and time = '%s';" %(timestamp,symbol,date,hhmm)
        dbcur.execute(sql)
        #print("sql is: \n%s",sql)
        
    conn.commit()



def alter():

    #alter_kline()
    alter_mline()


if __name__ == '__main__':

    alter()



        
