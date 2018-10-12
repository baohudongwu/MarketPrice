import common as c
import hq as hq
import time
import pandas as pd
import tushare as ts
import util as u
import datetime
import os
from operator import methodcaller
import lookuptest as lt
import newUtil as nu
import kline as kl
import threading
import tushare.util.formula as tsu


#hq.get_com()
# print("1")
# hq.toDB_record('get_shrink',u.get_shrink(0),"DAY="+str(c.DAYS)+",SHRINK_P=1.0101")
# print("2")
# uod='u'
# hq.toDB_record('get_soared',u.get_soared(0,uod),"uod ="+uod+",day="+str(c.DAYS)+",m5_10 ="+ str(c.m5_10)+",m10_20 = "+str(c.m10_20)+",TURNOVER="+str(c.TURNOVER)+",VOL="+str(c.VOL)+",UPSH="+str(c.UPSD_RATIO))
# print("3")
# d = {"5":"10","10":"15", "15":"20","20":"25","25":"30","30":"9999"}
# for key,value in d.items():
#     hq.toDB_record('get_aoi', u.get_aoi(2, top=value, low=key),"low=" + key+",top=" + value+",day="+str(c.DAYS))
# print("4")
# uod='d'
# if(u.get_soared(0,uod) != 'NULL'):
#     hq.toDB_record('get_soared',u.get_soared(0,uod),"uod ="+uod+",day="+str(c.DAYS)+",m5_10 ="+ str(c.m5_10)+",m10_20 = "+str(c.m10_20)+",TURNOVER="+str(c.TURNOVER)+",VOL="+str(c.VOL)+",DWSH="+str(c.DOWNSD_RATIO))
# print("5")
# hq.toDB_record('get_hopeDOJI', u.get_hopeDOJI(), "DOJI=" + str(c.DOJI))

# conn = ts.get_apis()
# #df=ts.bar('000001', conn=conn, freq='D', start_date=c.DATE, end_date=c.DATE,adj='qfq', ma=[5, 10, 20, 30,60,120],factors=['tor', 'vr'])
# df = ts.bar('000560', conn=conn, freq='D', start_date=c.DATE, end_date=c.DATE,adj='qfq')
# # df = ts.bar(code = '10001405',conn=conn,asset='X')
# print(df)
# ts.close_apis(conn)

# hq.toDB_get_periodata('D')
# hq.get_kline('D')




