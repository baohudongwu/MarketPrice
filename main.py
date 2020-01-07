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
import pro_hq as pro_hq
import importEntrust as ith
import time

# print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
# hq.get_com()
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
# if(u.get_hopeDOJI()!='NULL'):
#     hq.toDB_record('get_hopeDOJI', u.get_hopeDOJI(), "DOJI=" + str(c.DOJI))
#
# start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# pro_hq.toDB_pro_common()
# pro_hq.get_pro_kline(c.DATE.replace('-',''))
# end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# print('START   '+start)
# print('END   '+end)


#
# df = pd.read_excel("C:/Users/wyq/Desktop/123.xlsx",sheet_name='123')
# df.to_sql('t_stockentrust', c.ENGINE, if_exists='append')


# ts.set_token('37dd55b1cdf9e94548a9731821cdaf49c0d04be8ac6d038877a7341e')
# pro = ts.pro_api()
# #df = pro.index_weight(index_code='000016.SH', end_date='20191101')
# df = pro.concept()
# # df['ts_code'] = df['ts_code'].map(c.PRO_CODE_FORMAT)
# # df.to_sql('t_pro_optbasic', c.ENGINE, if_exists='append')
# print(df)


# ts.set_token('37dd55b1cdf9e94548a9731821cdaf49c0d04be8ac6d038877a7341e')
# pro = ts.pro_api()
# # df = pro.fund_daily(ts_code='510050.SH', start_date='20190628', end_date='20191208')
# df = pro.trade_cal(exchange='', start_date='20200101', end_date='20201231')
# # df['ts_code'] = df['ts_code'].map(c.PRO_CODE_FORMAT)
# #df.to_sql('t_pro_tradeday', c.ENGINE, if_exists='append')
# print(df)



#df.to_sql('t_pro_fundbasic', c.ENGINE, if_exists='append')

#计算时间间隔
# def get_lasttradeday(id,date,days):
#     SQL_LASTTRADEDAY = "SELECT cal_date from t_pro_tradeday where  is_open = 1 and cal_date >'" + date + "'"
#     print(SQL_LASTTRADEDAY)
#     df = pd.DataFrame(hq._excutesql(SQL_LASTTRADEDAY).fetchall())
#     df = df.reset_index()
#     print(df)
#     SQL = "update tmp set newdate ='" + df[0][days-1] + "' WHERE id = "+ str(id)
#     print(SQL)
#     #hq._excutesql(SQL)
#
# for id,date,days in hq._excutesql("select id,date,days from tmp order by id"):
#     try:
#         get_lasttradeday(id,date,days)
#     except:
#         continue

#print(u.get_shrink(15))
ith.importcsv_excle('20200106')

