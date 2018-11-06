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

#日线
def toDB_pro_daily():
    print("toDB_pro_daily start " + str(datetime.datetime.now()))
    df = pro.daily(trade_date=c.DATE.replace('-',''))
    df['ts_code'] = df['ts_code'].map(c.PRO_CODE_FOMART)
    df.to_sql('t_pro_daily', c.ENGINE, if_exists='append')

#处理5,10,20,30,60,120日线，量比 pro接口
def get_pro_kline():
    start = datetime.datetime.now()
    print(start)
    hq._excutesql("delete from t_kline")
    sql_body = "SELECT trade_date,ts_code,close,vol FROM t_pro_daily WHERE  ts_code = '"
    sql_foot = "' and trade_date <= '"+c.DATE.replace('-','')+"' order by trade_date desc limit 120"
    sql_u = "update t_pro_daily a,t_kline b set a.vr=b.vr,a.ma5=b.ma5,a.ma10=b.ma10,a.ma20=b.ma20,a.ma30=b.ma30,a.ma60=b.ma60,a.ma120=b.ma120 where a.trade_date =b.datetime and a.ts_code =b.code"
    def calc(market,sql):
        if(market == 'SH'):
            sql = "select code from T_StockBasics where code like '6%%' order by code "
        if (market == 'SZ'):
            sql = "select code from T_StockBasics where code like '0%%' order by code "
        if (market == 'CYB'):
            sql = "select code from T_StockBasics where code like '3%%' order by code "
        for row in hq._excutesql(sql):
            print(row['code'])
            sql = sql_body + row['code'] + sql_foot
            print(sql)
            df = pd.DataFrame(hq._excutesql(sql).fetchall())
            if (df.empty):
                print('empty')
                continue
            else:
                df.columns = ['datetime', 'code', 'close', 'vol']
                df['vol5'] = tsu.MA(df['vol'], 5)
                df['mean'] = df['vol5'].shift(-5)
                df['vr'] = df['vol'] / df['mean']
                df['vr'] = df['vr'].map(c.FORMAT)
                df['vr'] = df['vr'].astype(float)
                for a in c.MA:
                    if isinstance(a, int):
                        df['ma%s' % a] = tsu.MA(df['close'], a).map(c.FORMAT).shift(-(a - 1))
                df1 = df.drop(['close','vol','vol5','mean'], axis=1)
                df1.ix[:0].to_sql('t_kline', c.ENGINE, if_exists='append')
    try:
        t_sh = threading.Thread(target=calc, args=("SH", "sql"))
        t_sz = threading.Thread(target=calc, args=("SZ", "sql"))
        t_cyb = threading.Thread(target=calc, args=("CYB", "sql"))
        for t in [t_sh, t_sz, t_cyb]:
            t.start()
        for t in [t_sh, t_sz, t_cyb]:
            t.join()
    except Exception as e:
        print(e)
    print("############### UPDATE STARTING ###############")
    hq._excutesql(sql_u)
    end = datetime.datetime.now()
    print("get_pro_kline: " + str(end - start))

