'''
1.多头排列时（5日>10日>20日>30日），买入
2.多头排列后期，5日均线回落下叉（死叉）10日均线，卖出
3.空头排列时，保持空仓
4.空头排列后期，10日均线上升超过（金叉）20日均线，买入
5.均线纠缠（10日与20日太过靠近，或20日与30日线太过靠近，第1种情况不操作。
'''

import common as c
import hq as hq
import pandas as pd
import numpy as np
import datetime
import math
'''
1 多头排列
2 成交量放大 
3 换手率放大
4 成交量是在指定日期是最大
5 非长上影 (high-max（open，close)/close>=UPSD_RATIO
上影 high-max(open,close)/close
下影 min(open,close)-low/close
uod up or down 
'''
def get_soared(d,uod):
    start = datetime.datetime.now()
    if (uod=='u'):
        sql = c.SQL_SOARED
    else:
        sql = c.SQL_FALL
    df = pd.DataFrame(hq._excutesql(sql).fetchall())
    print(sql)
    list = []
    if (df.empty):
        return list
    else :
        df.columns = ['date', 'code', 'volume', 'turnover','open','close','high','low']
        tmp = 0
        i = 0
        df_max = df.groupby(['code'])['volume'].max()
        for code in df_max.index:
            i = np.where((df['code'] == code) & (df['volume']==df_max[code]))[0][0]
            high = df['high'][i]
            open = df['open'][i]
            close = df['close'][i]
            low = df['low'][i]
            if (uod=='u'):
                if(open>close):
                    tmp = open
                else:
                    tmp = close
                if (df['volume'][i]==df_max[code] and (high-tmp)/close<c.UPSD_RATIO):
                    if (df['date'][i] == hq.get_lasttradeday(d)):
                        # if (df['volume'][i] / df['volume'][i - 1] > float(c.VOL) and df['turnover'][i] / df['turnover'][
                        #     i - 1] > float(c.TURNOVER)):
                        list.append(df['code'][i])
            else:
                if (open < close):
                    tmp = open
                else:
                    tmp = close
                if (df['volume'][i] == max and (tmp - low) / close > c.DOWNSD_RATIO):
                    if (df['date'][i] == hq.get_lasttradeday(d)):
                        list.append([df['date'][i], df['code'][i]])
        end = datetime.datetime.now()
        print("running: "+str(end-start))
        #
        return  list

'''希望之星 hope doji
1 下跌趋势
2 当日十字星
3 十字星前一日实体阴线
4 后一日大阳，成交量是前两日之和 (目前是当日成交量比强两日大于黄金分割)
'''
def get_hopeDOJI():
    start = datetime.datetime.now()
    df = pd.DataFrame(hq._excutesql(c.SQL_DOJI).fetchall())
    #print(c.SQL_DOJI)
    list = []
    if (df.empty):
        return "NULL"
    else :
        df.columns = ['date', 'code', 'volume','p_change']
        df_max=df.groupby(['code'])['volume'].max()
        df_sum =df.groupby(['code'])['volume'].sum()
        for code in df_max.index:
            i = np.where((df['code'] == code) & (df['volume'] == df_max[code]))[0][0]
            if (df['date'][i] == c.DATE):
                if (df['volume'][i] == df_max[code] and df['p_change'][i]>0):
                    if (df_max[code]/(df_sum[code]-df_max[code])>c.DOJI):
                        list.append(code)
        end = datetime.datetime.now()
        print("running: " + str(end - start))
        # if(len(list)>0):
        #     return list
        # else:
        #     return "NULL"
        return list

'''
1 缩量股票 
'''
def get_shrink(d):
    start = datetime.datetime.now()
    df = pd.DataFrame(hq._excutesql(c.SQL_VSHRINK).fetchall())
    print(c.SQL_VSHRINK)
    list = []
    if (df.empty):
        print('empty')
        return list
    else:
        df.columns = ['date', 'code', 'volume','p_change']
        df_min = df.groupby(['code'])['volume'].min()
        #max = df.groupby(['code'])['volume'].max()
        #mean = df.groupby(['code'])['volume'].mean()
        for code in df_min.index:
            i = np.where((df['code'] == code) & (df['volume'] == df_min[code]))[0][0]
            if (df['volume'][i]==df_min[code] and df['date'][i]==hq.get_lasttradeday(d)):
                list.append(df['code'][i])
        end = datetime.datetime.now()
        print("running: " + str(end - start))
        # if (len(list) > 0):
        #     return list
        # else:
        #     return "NULL"
        return list

#涨幅选股 amount of increase;
'''
def get_gradient():
    df = pd.DataFrame(hq._excutesql(c.SQL_GRADIENT).fetchall())
    print(c.SQL_GRADIENT)
    list = []
    min = 0
    if (df.empty):
        print('empty')
        return list
    else:
        
        df_date = pd.DataFrame(hq._excutesql(c.SQL_TRADEDAY).fetchall())
        df.columns = ['date', 'code','high','low']
        df_CURRENT = df[df['date']==c.DATE]
        # x = np.arange(c.DAYS)
        # xx = pd.DataFrame({"date":x})
        df_min = pd.DataFrame(columns=['date', 'code', 'low'])
        min = df.groupby(['code'])['low'].min()
        for i in min.index:
            for j in np.where((df['low']==min[i]))[0]:
                if(df['code'][j]==i):
                    print(df[df['date']>df['date'][j] and df['code'][j]==i])



        return list
'''
#**kwargs [2]:top=,low=;[1] top=
def get_aoi(type,**kwargs):
    start = datetime.datetime.now()
    df = pd.DataFrame(hq._excutesql(c.SQL_GRADIENT).fetchall())
    #print(c.SQL_GRADIENT)
    list = []
    min = 0
    if (df.empty):
        print('empty')
        return list
    else:
        df.columns = ['date', 'code', 'high', 'low']
        df_CURRENT = pd.DataFrame(hq._excutesql(c.SQL_TRADEDAY).fetchall())
        df_code = df['code'].drop_duplicates()
        for key, value in kwargs.items():
            if (key == 'top'):
                TOP = value
            else:
                LOW = value
        for code in df_code:
            df_tmp=df[df['code']==code]
            min = df_tmp.groupby(['code'])['low'].min()
            max = df_tmp.groupby(['code'])['low'].max()
            if (type == 1):
                condition = "(max[0]-min[0])/min[0]*100>"+str(TOP)
            else:
                condition = "(max[0] - min[0]) / min[0] * 100 > "+str(LOW)+" and (max[0] - min[0]) / min[0] * 100 < "+str(TOP)
            if(eval(condition)):
                str_max = str(df_tmp[df_tmp['low'] == max[0]]['date'])
                str_min = str(df_tmp[df_tmp['low'] == min[0]]['date'])
                date_max = str_max[len(str_max) - 36:len(str_max) - 26]
                date_min = str_min[len(str_min) - 36:len(str_min) - 26]
                int_datemin = np.where((df_CURRENT[1] == date_min))
                int_datecur = np.where((df_CURRENT[1] == c.DATE))
                if ((int_datecur[0][0] - int_datemin[0][0]) > (float(c.DAYS) * 0.618)):
                    if (date_max == c.DATE):
                        list.append(code)
        end = datetime.datetime.now()
        print("running: " + str(end - start))
        return list

'''
平台突破
1、上升通道股票
2、T日收盘价大于五日线
3、T日前某日跳空且随后几日没破五日线或十日线：（目前暂定10日内）
4、两种情况：a.已经经过平台期，即已经接近5日线或已经在10日线支撑;b.刚爆发彪离5日线
5、数据源:T日收盘价高于5日线，均线发散的股票
6、先找到爆发日，以爆发日为原点连续数日没有达到5日线
'''
def get_platform():
    start = datetime.datetime.now()
    df = pd.DataFrame(hq._excutesql(c.SQL_PLATFORM).fetchall())
    print(c.SQL_PLATFORM)
    list = []
    if (df.empty):
        print('empty')
        return list
    else:
        #先判断是否进入平台期 date,code,close,high,low,ma5,ma10,ma20,p_change,volume,turnover
        df.columns = ['date', 'code','close', 'high', 'low','ma5','ma10','ma20','p_change','volume','turnover']
        df_fly = df[(df['date']==c.DATE) & (df['low']>df['ma5'])] #飞龙在天的
        df_plat = df[(df['date']==c.DATE) & ((df['low']<df['ma5']) | (df['low']==df['ma5']))] #已经着落的
        max_fly_high = df_fly.groupby(['code'])['high'].max()
        max_plat_high = df_plat.groupby(['code'])['high'].max()

#周线,月线级别的上升
def get_rise(period):
    start = datetime.datetime.now()
    list = []
    i=j =0
    if period=="M":
        sql = c.SQL_MONTH
        cir = 3
        com = 2
    else:
        sql = c.SQL_WEEK
        cir = 4
        com = 3
    print(sql)
    df = pd.DataFrame(hq._excutesql(sql).fetchall())
    df.columns = ['date', 'code', 'close', 'high', 'low', 'vol']
    df_high_max = df.groupby(['code'])['high'].max()
    df_low_max = df.groupby(['code'])['low'].max()
    df_date_max = df.groupby(['code'])['date'].max()[0]
    for code in df_high_max.index:
        count = 0
        i = np.where((df['code'] == code) & (df['high'] == df_high_max[code]))[0][0]
        j = np.where((df['code'] == code) & (df['low'] == df_low_max[code]))[0][0]
        if (i==j and df['date'][i]==df_date_max and len(df[df['code']==code])>1):
            for m in range(cir):
                if (df['high'][i]/df['high'][i-1]>1.006):
                    count = count +1
                i = i -1
        if (count>com):
            list.append(code)
    end = datetime.datetime.now()
    print("running: " + str(end - start))
    return list



