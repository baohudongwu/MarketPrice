import common as c
import tushare as ts
import hq as hq
import pandas as pd
import numpy as np
import datetime
from pandas.core.frame import DataFrame
import tushare.util.formula as tsu
import threading

ts.set_token('37dd55b1cdf9e94548a9731821cdaf49c0d04be8ac6d038877a7341e')
global pro
pro = ts.pro_api()

#股票列表
def toDB_pro_stocklist():
    print("toDB_pro_stocklist start "+str(datetime.datetime.now()))
    sql = 'delete from t_pro_stocklist'
    hq._excutesql(sql)
    pro.stock_basic(exchange_id='', fields='ts_code,symbol,name,list_date,delist_date,list_status').to_sql('t_pro_stocklist', c.ENGINE, if_exists='append')
    print("toDB_pro_stocklist end " + str(datetime.datetime.now()))

#分红数据
def toDB_pro_dividend():
    print("toDB_pro_dividend start " + str(datetime.datetime.now()))
    sql = 'delete from t_pro_dividend'
    hq._excutesql(sql)
    for row in hq._excutesql(c.PRO_STOCK_LIST):
        df = pro.dividend(ts_code=row[0])
        if df is None:
            pass
        else:
            df['ts_code'] =df['ts_code'].map(c.PRO_CODE_FOMART)
            df.to_sql('t_pro_dividend', c.ENGINE, if_exists='append')
    print("toDB_pro_dividend end " + str(datetime.datetime.now()))

toDB_pro_dividend()