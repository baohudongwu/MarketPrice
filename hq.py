import common as c
import tushare as ts
import pandas as pd
import numpy as np
import datetime
from pandas.core.frame import DataFrame
import tushare.util.formula as tsu
import threading

def _excutesql(sql):
    connection = c.ENGINE.connect()
    try:
        result = connection.execute(sql)
    except:
        result = None
    finally:
        connection.close()
    return result

def Add_col(dt, **kwargs):
    for key, value in kwargs.items():
        dt.insert(0, key, value)
    return dt

def toDB_tradeday():
    ts.trade_cal().to_sql('t_tradeday', c.ENGINE, if_exists='append')

def istradeday(date):
    df = pd.DataFrame(_excutesql(c.SQL_TRADEDAY).fetchall())
    for i in range(len(df)):
        if (df[1][i] == date):
            flag = 1
            return date
        else:
            flag = 0
    if (flag == 0):
        return pd.DataFrame(_excutesql(
            "SELECT max(calendardate) from t_tradeday where  isopen = 1 and calendardate <'" + date + "'").fetchall())[
            0][0]

# 指定日期的上day的交易日
def get_lasttradeday(days):
    df = pd.DataFrame(_excutesql(c.SQL_TRADEDAY).fetchall())
    flag = 0
    if (days == 0):
        return c.DATE
    else:
        for i in range(len(df)):
            if (df[1][i] == c.DATE):
                flag = 1
                return df[1][i - days]
            else:
                flag = 0
        if (flag == 0):
            tmp = pd.DataFrame(_excutesql(c.SQL_LASTTRADEDAY).fetchall())[0][0]
            for j in range(len(df)):
                if (df[1][j] == tmp):
                    return df[1][j - days]

# 相差交易日天数,date1,date2均为交易日
def get_days(date1, date2, df):
    # df = pd.DataFrame(_excutesql(c.SQL_TRADEDAY).fetchall())
    int_date1 = np.where((df[1] == date1))
    int_date2 = np.where((df[1] == date2))
    return (int_date2[0][0] - int_date1[0][0])

# 1 is code
def toDB_record(functionname, list, remark):
    try:
        start = datetime.datetime.now()
        if (functionname!='get_rise'):
            _excutesql("delete from t_record where date=" + c.DATE + " AND type=" + functionname)
    finally:
        if  list :
            df = DataFrame(list)
            df.columns = ['code']
            df.insert(0, 'date', c.DATE)
            df.insert(2, 'type', functionname)
            df.insert(3, 'remark', remark)
            df.to_sql('t_record', c.ENGINE, if_exists='append')
        end = datetime.datetime.now()
        print("TODB: " + str(end - start))

# 函数与表名
def toDB_Funcitonname(fname, tname, type, comment):
    sql = "INSERT INTO t_functionmap(fname,tname,type,comment) values('" + fname + "','" + tname + "'," + str(
        type) + ",'" + comment + "')"
    _excutesql(sql)

# 通用落地数据(非bar接口)
# 0-直接调用接口，表不需要整理
# 1-需针对个股进行接口调用
# 2-针对个股调用接口，并增加code字段
# 7-调用接口，需有日期参数
# 8-针对个股调用接口，增加date字段
# 9-针对个股调用接口，增加year,quarter字段
def get_com():
    start = datetime.datetime.now()
    for f, t, y in _excutesql("select fname,tname,type from t_functionmap where flag = 1"):
        print("###" + f + "###" + t + "###" + "###")
        try:
            if y == 0:
                sql = "delete from " + t
                _excutesql(sql)
                sql = "update t_functionmap set flag = 0 where tname ='" + t + "'"
                _excutesql(sql)
                eval(f).to_sql(t, c.ENGINE, if_exists='append')
            if y == 1:
                sql = "delete from " + t + " where date=" + c.DATE
                _excutesql(sql)
                sql = "update t_functionmap set flag = 0 where tname ='" + t + "'"
                _excutesql(sql)
                for row in _excutesql(c.SQL_CODE):
                    df = eval(f)(row['code'], start=c.DATE, end=c.DATE)
                    df.to_sql(t, c.ENGINE, if_exists='append')
            if y == 2:
                sql = "delete from " + t + " where date='" + c.DATE + "'"
                _excutesql(sql)
                sql = "update t_functionmap set flag = 0 where tname ='" + t + "'"
                _excutesql(sql)
                for row in _excutesql(c.SQL_CODE):
                    df = eval(f)(row['code'], start=c.DATE, end=c.DATE)
                    if df is None:
                        pass
                    else:
                        Add_col(df, code=row['code']).to_sql(t, c.ENGINE, if_exists='append')
            if y == 7:
                sql = "delete from " + t + " where date =" + c.DATE
                _excutesql(sql)
                sql = "update t_functionmap set flag = 0 where tname ='" + t + "'"
                _excutesql(sql)
                eval(f).to_sql(t, c.ENGINE, if_exists='append')
            if y == 8:
                sql = "delete from " + t + " where date =" + c.DATE
                _excutesql(sql)
                sql = "update t_functionmap set flag = 0 where tname ='" + t + "'"
                _excutesql(sql)
                Add_col(eval(f), date=c.DATE).to_sql(t, c.ENGINE, if_exists='append')
            if y == 9:
                sql = "delete from " + t + " where year =" + str(c.YEAR) + " and quarter=" + str(c.QUARTER)
                _excutesql(sql)
                sql = "update t_functionmap set flag = 0 where tname ='" + t + "'"
                _excutesql(sql)
                df = eval(f)(c.YEAR, c.QUARTER)
                Add_col(df, year=c.YEAR, quarter=c.QUARTER).to_sql(t, c.ENGINE, if_exists='append')
        except:
            sql = "update t_functionmap set flag = 3 where tname ='" + t + "'"
            _excutesql(sql)
            continue
        finally:
            sql = "update t_functionmap set flag = 1 where flag = 0"
            _excutesql(sql)
            end = datetime.datetime.now()
            print("get_com: " + str(end - start))

# 大盘历史数据
def toDB_his_dp():
    for row in ['sh', 'sz', 'hs300', 'sz50', 'zxb', 'cyb']:
        df = ts.get_k_data(row)
        df.to_sql('t_hisdata', c.ENGINE, if_exists='append')

# 后复权 backward adjusted price
def toDB_backward_adjusted_price():
    for row in _excutesql(c.SQL_CODE):
        df = ts.get_h_data(row['code'], autype='hfq')
        if df is None:
            pass
        else:
            Add_col(df, code=row['code']).to_sql('t_AjustedPrice_back', c.ENGINE, if_exists='append')

def toDB_cap_tops(date, days):
    df = ts.cap_tops()
    Add_col(df, date=date, days=days).to_sql('t_cap_tops', c.ENGINE, if_exists='append')

#bar接口数据
def toDB_get_periodata(period):
    start = datetime.datetime.now()
    print(start)
    global CONS
    CONS = ts.get_apis()
    if period == 'W':
        freq = 'W'
        table = 't_weekdata'
        command = "ts.bar(row['code'], conn=CONS, freq=freq, start_date=key, end_date=value,adj = 'qfq',factors=['tor']).to_sql(table, c.ENGINE, if_exists='append')"
    if period == 'M':
        freq = 'M'
        table = 't_monthdata'
        command = "ts.bar(row['code'], conn=CONS, freq=freq, start_date=key, end_date=value, adj = 'qfq',factors=['tor']).to_sql(table, c.ENGINE, if_exists='append')"
    try:

        if (period == 'D'):
            sql = "delete from t_daydata where datetime ='"+c.DATE+"'"
            _excutesql(sql)
            def tmp(con,sql):
                if (con == 'CONN_SH'):
                    sql = "select code from T_StockBasics where code like '6%%' order by code"
                if (con == 'CONN_SZ'):
                    sql = "select code from T_StockBasics where code like '0%%' order by code"
                if (con == 'CONN'):
                    sql = "select code from T_StockBasics where code like '3%%' order by code"
                com = "ts.bar(row['code'], conn=CONS, freq='D', start_date=c.DATE, end_date=c.DATE,adj = 'qfq',factors=['tor']).to_sql('t_daydata', c.ENGINE, if_exists='append')"
                for row in _excutesql(sql):
                    try:
                        print(row)
                        eval(com)
                    except Exception as e:
                        print('error')
                        continue
            try:
                t_sh = threading.Thread(target=tmp,args=("CONN_SH","sql"))
                t_sz = threading.Thread(target=tmp,args=("CONN_SZ","sql"))
                t_cyb = threading.Thread(target=tmp,args=("CONN","sql"))
                for t in [t_sh,t_sz,t_cyb]:
                    t.start()
                for t in [t_sh,t_sz,t_cyb]:
                    t.join()
            except Exception as e:
                print(e)
        else:
            for row in _excutesql(c.SQL_CODE):
                for key, value in c.PERIOD.items():
                    try:
                        print(row)
                        eval(command)
                    except Exception as e:
                        print(e)
                        continue
    finally:
        ts.close_apis(CONS)
    end = datetime.datetime.now()
    print("toDB_get_periodata: " + str(end - start))

#处理5,10,20,30,60,120日线，量比
def get_kline(period):
    start = datetime.datetime.now()
    print(start)
    _excutesql("delete from t_kline")
    if period == 'D':
        sql_table = 't_daydata'
    if period =='M':
        sql_table = 't_monthdata'
    if period =='W':
        sql_table = 't_weekdata'
    #sql_body = "SELECT datetime,code,close,vol FROM "+sql_table+" WHERE  vol not like '%%e%%' and amount not like '%%e%%' and code = '"
    sql_body = "SELECT datetime,code,close,vol FROM " + sql_table + " WHERE  vol not like '%%e%%' and amount not like '%%e%%' and code = '"
    sql_foot = "' and datetime <= '"+c.DATE+"' order by datetime desc limit 120"
    sql_u = "update "+sql_table+" a,t_kline b set a.vr=b.vr,a.ma5=b.ma5,a.ma10=b.ma10,a.ma20=b.ma20,a.ma30=b.ma30,a.ma60=b.ma60,a.ma120=b.ma120 where a.datetime =b.datetime and a.code =b.code"
    def calc(market,sql):
        if(market == 'SH'):
            sql = "select code from T_StockBasics where code like '6%%' order by code "
        if (market == 'SZ'):
            sql = "select code from T_StockBasics where code like '0%%' order by code "
        if (market == 'CYB'):
            sql = "select code from T_StockBasics where code like '3%%' order by code "
        for row in _excutesql(sql):
            print(row['code'])
            sql = sql_body + row['code'] + sql_foot
            df = pd.DataFrame(_excutesql(sql).fetchall())
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
    _excutesql(sql_u)
    end = datetime.datetime.now()
    print("get_kline: " + str(end - start))