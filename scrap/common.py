#! /usr/local/bin/python3
import pymysql

def format_time(time_str):
    '''
    yyyy-mm-dd hh:MM:ss => yyyymmddhhMMss
    '''
    if len(time_str) >= 19:
        time_str = time_str.replace("-","").replace(":","")        
        time_str = time_str.replace(" ","")
        
    return time_str

def secureValue(item,index):

    if item[index] == None:
        return 0
    return item[index]


    
def secureDictValue(dictData, key,defaultValue = None):

    if key in dictData :
        return dictData[key]
    return defaultValue

#format_time("2016-09-30 15:00:10")
def initDatabase():
    
    try:
        conn = pymysql.connect(host='localhost',user='root',password='',db='stock')
        return conn
    except IOError as e:
        print("mysql error : ",e)
        exit(-1)

def store_kline_data(conn,symbol , klist , fq , ktype):
    
    dbcur = conn.cursor()
    
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
    datetime = format_time(datetime)
    
    sql = "select * from quote where symbol = '%s' and time = '%s'; " % (symbol , datetime)
    count = dbcur.execute(sql)
    if count > 0 :
        return
    
    timestamp = secureDictValue(data,"mtime",0)
    high = secureDictValue(data,"high",0)
    low = secureDictValue(data,"low",0)
    preclose = secureDictValue(data,"preclose",0)
    open_price = secureDictValue(data,"open",0)
    last = secureDictValue(data,"last",0)
    vol = secureDictValue(data,"vol",0)
    amt = secureDictValue(data,"amt",0)
    qrr = secureDictValue(data,"qrr",0)
    diff = secureDictValue(data,"diff",0)
    change = secureDictValue(data,"change",0)
    amp = secureDictValue(data,"amp",0)
    trade = secureDictValue(data,"tr",0)
    pe = secureDictValue(data,"pe",0)
    pb = secureDictValue(data,"pb",0)
    status = secureDictValue(data,"","L")
    mc  = secureDictValue(data,"mc",0)
    cmc  = secureDictValue(data,"cmc",0)
    tso  = secureDictValue(data,"tso",0)
    shares  = secureDictValue(data,"shares",0)
    b1p = secureDictValue(data,"b1p",0)
    b1v = secureDictValue(data,"b1v",0)
    b2p = secureDictValue(data,"b2p",0)
    b2v = secureDictValue(data,"b2v",0)
    b3p = secureDictValue(data,"b3p",0)
    b3v = secureDictValue(data,"b3v",0)
    b4p = secureDictValue(data,"b4p",0)
    b4v = secureDictValue(data,"b4v",0)
    b5p = secureDictValue(data,"b5p",0)
    b5v = secureDictValue(data,"b5v",0)
    s1p = secureDictValue(data,"s1p",0)
    s1v = secureDictValue(data,"s1v",0)
    s2p = secureDictValue(data,"s2p",0)
    s2v = secureDictValue(data,"s2v",0)
    s3p = secureDictValue(data,"s3p",0)
    s3v = secureDictValue(data,"s3v",0)
    s4p = secureDictValue(data,"s4p",0)
    s4v = secureDictValue(data,"s4v",0)
    s5p = secureDictValue(data,"s5p",0)
    s5v = secureDictValue(data,"s5v",0)

    sql = "insert into quote(symbol , time , high , low , preclose , open , last , volume , amount , qrr , timestamp , diff , price_change , amp , trade , pe , pb , status , mc , cmc , tso , share , bid_1_price , bid_1_volume , bid_2_price , bid_2_volume,bid_3_price,bid_3_volume , bid_4_price , bid_4_volume , bid_5_price , bid_5_volume , sell_1_price , sell_1_volume , sell_2_price , sell_2_volume , sell_3_price , sell_3_volume , sell_4_price , sell_4_volume , sell_5_price , sell_5_volume) values('%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,'%s',%f,%f,%f,%f, %f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f )" %(symbol,datetime,high,low,preclose,open_price,last,vol,amt,qrr,timestamp,diff,change,amp,trade,pe,pb,status,mc,cmc,tso,shares,b1p,b1v,b2p,b2v,b3p,b3v,b4p,b4v,b5p,b5v,s1p,s1v,s2p,s2v,s3p,s3v,s4p,s4v,s5p,s5v)
    
    
    dbcur.execute(sql)
    
    conn.commit()

def store_mline_data(conn,symbol,data):

    dbcur = conn.cursor()
    date = data["date"]
    syms = data["list"]
    for d in syms :
        t = d[0] # time
        p = d[1] #price
        v = d[2] #volume
        #print(" %s %f %f "%(t,p,v))
        sql = "select symbol from mline where symbol = '%s' and date = '%s' and time = '%s';" %(symbol , date , t)
        #print("sql is: ",sql)
        count = dbcur.execute(sql)
        if count > 0:
            #print("has this data\n")
            continue
        sql = "insert into mline(symbol,date,time,price,volume) values('%s','%s','%s',%f,%f)" %(symbol,date,t,p,v)
        #print("sql is: ",sql)
        dbcur = conn.cursor()
        dbcur.execute(sql)
    
    conn.commit()
