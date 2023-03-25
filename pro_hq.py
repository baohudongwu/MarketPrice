import common as c
import tushare as ts
import hq as hq
import pandas as pd
import numpy as np
import datetime
from pandas.core.frame import DataFrame
import tushare.util.formula as tsu
import threading
import time
import math
from _pydecimal import Decimal,Context,ROUND_HALF_UP


ts.set_token('37dd55b1cdf9e94548a9731821cdaf49c0d04be8ac6d038877a7341e')
global pro
pro = ts.pro_api()

def toDB_pro_funcitonname(fname, tname, type, comment):
    sql = "INSERT INTO t_pro_functionmap(fname,tname,type,comment) values('" + fname + "','" + tname + "'," + str(
        type) + ",'" + comment + "')"
    hq._excutesql(sql)

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
            df['ts_code'] =df['ts_code'].map(c.PRO_CODE_FORMAT)
            df.to_sql('t_pro_dividend', c.ENGINE, if_exists='append')
    print("toDB_pro_dividend end " + str(datetime.datetime.now()))

#概念股票明细
def toDB_pro_conceptDetail():
    for id,name in hq._excutesql("select code,name from t_pro_concept order by code"):
        df = pro.concept_detail(id=id[0], fields='ts_code,name,in_date,out_date')
        if df is None:
            pass
        else:
            df['ts_code'] =df['ts_code'].map(c.PRO_CODE_FORMAT)
            hq.Add_col(df,id=id,concept=name)
            time.sleep(10)
            df.to_sql('t_pro_concept_detail', c.ENGINE, if_exists='append')
    print("toDB_pro_conceptDetail end " + str(datetime.datetime.now()))

#tpye 0 使用query方法且参数需要日期
#type 1 方法参数存储在库中
#type 2 无入参
def toDB_pro_common():
    start = datetime.datetime.now()
    for fn, tn, t,i,para in hq._excutesql("select fname,tname,type,isdate,parameter from t_pro_functionmap where flag = 1 and isusual = 'Y'"):
        print("###" + fn + "###" + tn + "###" + "###")
        try:
            if i == 'Y':
                sql_del = "delete from " + tn + " where trade_date = '" + c.DATE.replace('-', '') + "'"
            else:
                sql_del = "delete from " + tn
            #print(sql_del)
            hq._excutesql(sql_del)
            sql_update = "update t_pro_functionmap set flag = 0 where tname ='" + tn + "'"
            hq._excutesql(sql_update)
            if t == 0:
                df = pro.query(fn, trade_date=c.DATE.replace('-', ''))
            if t == 1:
                fun = "pro."+fn +'('+para+')'
                df = eval(fun)
            try:
                if (fn=='index_weight'):
                    df['con_code'] = df['con_code'].map(c.PRO_CODE_FORMAT)
                else:
                    df['ts_code'] = df['ts_code'].map(c.PRO_CODE_FORMAT)
            finally:
                df.to_sql(tn, c.ENGINE, if_exists='append')
        except:
            sql = "update t_pro_functionmap set flag = 3 where tname ='" + tn + "'"
            print(sql)
            hq._excutesql(sql)
            continue
        finally:
            sql = "update t_pro_functionmap set flag = 1 where flag = 0 or flag = 3"
            hq._excutesql(sql)
            end = datetime.datetime.now()
            print("get_pro_com: " + str(end - start))

#处理5,10,20,30,60,120日线，量比 pro接口
def get_pro_kline(date):
    start = datetime.datetime.now()
    print(start)
    hq._excutesql("delete from t_kline")
    sql_body = "SELECT trade_date,ts_code,close,vol FROM t_pro_daily WHERE  ts_code = '"
    sql_foot = "' and trade_date <= '"+date+"' order by trade_date desc limit 120"
    sql_u = "update t_pro_daily a,t_kline b set a.vr=b.vr,a.ma5=b.ma5,a.ma10=b.ma10,a.ma20=b.ma20,a.ma30=b.ma30,a.ma60=b.ma60,a.ma120=b.ma120 where a.trade_date =b.datetime and a.ts_code =b.code"
    def calc(market,sql):
        if(market == 'SH'):
            sql = "select code from v_stockcode where code like '6%%' order by code "
        if (market == 'SZ'):
            sql = "select code from v_stockcode where code like '0%%' order by code "
        if (market == 'CYB'):
            sql = "select code from v_stockcode where code like '3%%' order by code "
        for row in hq._excutesql(sql):
            #print(row['code'])
            sql = sql_body + row['code'] + sql_foot
            df = pd.DataFrame(hq._excutesql(sql).fetchall())
            if (df.empty):
                #print(row['code'] + ' is empty')
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

#计算每天涨跌停数量
#不统计：第一日上市股票，统计：沪深，科创、创业涨跌停数量
def get_limit_number(date):
#print
#select convert(pre_close*1.1,decimal(10,2)) as top,convert(pre_close*0.9,decimal(10,2)) as down,close from t_pro_daily where trade_date = '20210420'
# select count(1),a.top from (
# select convert(pre_close*1.1,decimal(10,2))-close as top,convert(pre_close*0.9,decimal(10,2))-close as down from t_pro_daily where trade_date = '20210420' and (ts_code like '0%' or ts_code like '60%') and ts_code not in (select ts_code from v_ststocklist)
# ) as a where a.top=0
    SQL = "select ts_code,pre_close,`close`,high,low from t_pro_daily a where not EXISTS (select 1 from v_ststocklist b where a.ts_code = b.ts_code)  and  trade_date = '"+date+"'"
    SQL_ST = "select ts_code,pre_close,`close`,high,low from t_pro_daily a where  EXISTS (select 1 from v_ststocklist b where a.ts_code = b.ts_code)  and  trade_date = '"+date+"'"
    df_5 = pd.DataFrame(hq._excutesql(SQL_ST).fetchall())
    df_5.columns=['code','pre_close','close','high','low']
    df_5['date']=date
    df_5['top']=(df_5['pre_close']*1.05-df_5['close']).map(lambda x:round(x,2))
    df_5['down']=(df_5['pre_close']*0.95-df_5['close']).map(lambda x:round(x,2))
    df_5['top_d']=(df_5['pre_close']*1.05-df_5['high']).map(lambda x:round(x,2))
    df_5['low_d']=(df_5['pre_close']*0.95-df_5['low']).map(lambda x:round(x,2))
    df =pd.DataFrame(hq._excutesql(SQL).fetchall())
    df.columns=['code','pre_close','close','high','low']
    df['date']=date
    df_10 = df[(df['code'].map(lambda x:x[0:1])=='0') | (df['code'].map(lambda x: x[0:2])=='60')]
    df_10['top']=(df_10['pre_close']*1.1-df_10['close']).map(lambda x:round(x,2)) #封住涨停
    df_10['down']=(df_10['pre_close']*0.9-df_10['close']).map(lambda x:round(x,2))
    df_10['top_d']=(df_10['pre_close']*1.1-df_10['high']).map(lambda x:round(x,2))#涨停过
    df_10['low_d']=(df_10['pre_close']*0.9-df_10['low']).map(lambda x:round(x,2))
    df_20 = df[(df['code'].map(lambda x:x[0:1])=='3') | (df['code'].map(lambda x: x[0:2])=='68')]
    df_20['top']=(df_20['pre_close']*1.2-df_20['close']).map(lambda x:round(x,2))
    df_20['down']=(df_20['pre_close']*0.8-df_20['close']).map(lambda x:round(x,2))
    df_20['top_d']=(df_20['pre_close']*1.2-df_20['high']).map(lambda x:round(x,2))
    df_20['low_d']=(df_20['pre_close']*0.8-df_20['low']).map(lambda x:round(x,2))
    df_all = pd.concat([df_5,df_10,df_20],axis=0,ignore_index=True)
    #print(df_10[df_10['code']=='600996'])
    #print(df_10[df_10['down']==0])
    #print(df_all[(df_all['top']==0) | (df_all['down']==0) | (df_all['high']==0) | (df_all['low']==0)])
    df_all[(df_all['top']==0) | (df_all['down']==0) | (df_all['top_d']==0) | (df_all['low_d']==0)].to_sql('t_limit_detail', c.ENGINE, if_exists='append')


