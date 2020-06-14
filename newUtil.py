import common as c
import hq as hq
import pandas as pd
import numpy as np
import datetime,time
import math

from sklearn import datasets, linear_model
import matplotlib.pyplot as plt
import talib
#
def get_soared_bar(d,uod):
    start = datetime.datetime.now()
    if (uod=='u'):
        sql = c.BAR_SOARED
    else:
        sql = c.BAR_FALL
    print(sql)
    df = pd.DataFrame(hq._excutesql(sql).fetchall())

    list = []
    if (df.empty):
        return list
    else :
        df.columns = ['date', 'code', 'volume', 'turnover','open','close','high','low','amount','vr']
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
                        if (df['volume'][i] / df['volume'][i - 1] > float(c.VOL) and df['turnover'][i] / df['turnover'][i - 1] > float(c.TURNOVER)):
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

#换手率

#吉登堡凶兆
'''
1 取周期
'''
def hindenburgomen():
    tcode={}
    codenumber=[]
    xcode,k,X,Y=[],[],[],[]
    Rsq=pd.DataFrame()

    index='000001'#上证指数
    sectorcode="2000032255"#上证A股成分
    startdate='20160101'#起始日期
    enddate='20161231'#截止日期
    #后推30日获取

    Ndate=c.getdate(enddate,30)
    enddateN30=Ndate.Dates[0]
    date=c.tradedates(startdate,enddateN30)

    #获取上证A股的板块成分，每日新增的代码也获取对应的收盘价数据
    for i in range(len(date.Dates)-30):
        if(i==0):
           code = c.sector(sectorcode, date.Dates[i])
           data=c.csd(code.Codes,"CLOSE",startdate,enddateN30,'Period=1,ispandas=1')
           codenumber=code.Codes
           code2=code
        else:
           code1=c.sector(sectorcode, date.Dates[i])
           addcode = [i for i in code1.Codes if i not in code2.Codes]
           if(len(addcode)!=0):
               adddata=c.csd(addcode,"CLOSE",startdate,enddateN30,'Period=1,ispandas=1')
               data=data.append(adddata)
               codenumber=codenumber+addcode
           code2 = code1
    data.to_csv('data.csv')
    data=pd.read_csv('data.csv')
    data=data.set_index('CODES')
    #获取对比指数收盘价数据
    indexdata=c.csd(index,"CLOSE",startdate,enddateN30,'Period=1,ispandas=1,rowindex=1')
    #板块成分和指数线性回归，并计算30日间隔的Rsq系数
    for i in range(len(codenumber)):
             for x, y in zip(data.ix[codenumber[i],"CLOSE"], indexdata["CLOSE"]):
                  X.append([float(x)])
                  Y.append(float(y))
             for j in range(0,len(Y)):
                   try:
                    regr = linear_model.LinearRegression()
                    regr=regr.fit(X[j:j+30], Y[j:j+30])
                    regr=regr.score(X[j:j + 30], Y[j:j + 30])
                    xcode.append(regr)
                    if(j+30>=len(Y)-1):
                      break
                   except:
                      xcode.append(0)
             tcode[codenumber[i]]=xcode
             xcode=[]
             X=[]
             Y=[]
    Rsq=pd.DataFrame(tcode)
    Rsq.to_csv('rsq.csv',index=None)
    Rsq=pd.read_csv('rsq.csv')
    #求每日Rsq系数的均值
    FORMAT ='%d/%02d/%02d'
    Rsq=Rsq.T
    mean=pd.DataFrame(Rsq.mean(),columns=['MEAN'])
    for i in range(Rsq.columns.size):
      mean['MEAN']=mean['MEAN']*len(Rsq)/len(Rsq[Rsq[i]!=0.0])
    mean['DATE']=date.Dates[30:]

    #求每日Rsq系数的均值间隔3日变动幅度超过30%的日期进行标记
    for i in range(len(mean)-33):
        x=(mean.ix[i,'MEAN']-mean.ix[i+3,'MEAN'])/mean.ix[i,'MEAN']
        if (x >0.3 or x<-0.3):
            k.append(mean.ix[i + 3, 'DATE'])
            list1=mean.ix[i + 3, 'DATE'].split("/")
            list1=FORMAT % (int(list1[0]), int(list1[1]), int(list1[2]))
            indexdata.ix[indexdata['DATES']==list1, 'MARKER'] = 1
        else:
            pass
    #绘制指数收盘价
    indexdata=indexdata.set_index('DATES')
    indexdata.index=pd.to_datetime(indexdata.index)
    #标记吉登堡凶兆所标记的时间点
    for i in range(0,len(indexdata.ix[indexdata["MARKER"]==1,"CLOSE"])):
       plt.scatter(pd.to_datetime((indexdata.ix[indexdata["MARKER"]==1,"CLOSE"]).index)[i],indexdata.ix[indexdata["MARKER"]==1,"CLOSE"][i], color='red',marker='o')
    indexdata['CLOSE'].plot(figsize=(10,8))
    plt.xlabel("date")
    plt.ylabel("close")
    plt.title(u"000001.SH")
    plt.show()


def MACD(price, fastperiod, slowperiod, signalperiod):
    ewma12 = pd.ewma(price,span=fastperiod)
    ewma60 = pd.ewma(price,span=slowperiod)
    dif = ewma12-ewma60
    dea = pd.ewma(dif,span=signalperiod)
    macd = (dif-dea)*2
    return dif,dea,macd

''' 底背离
1、日线数据[计算当日前120条数据]
2、满足条件：
a、下跌趋势
b、当股价最低时，MACD的DIFF>DEA；
c、追溯上一个DIFF>DEA时的那日最低价(price_lower)，如果最低价(price_lower)>当日最低价(price_lowest)则底背离成立
或者
a、下跌趋势
b、当股价最低时，MACD的DIFF不是最低；
或者
比较T日前120条数据，最低价越来越低，diff越来越高，2次背离

'''

def get_dbl(date):
    # PRO_DBL = "SELECT a.ts_code FROM t_pro_daily a left join t_pro_dailybasic b on a.ts_code = b.ts_code and a.trade_date =b.trade_date where a.ma5 < a.ma10 and a.ma10 <a.ma30 and a.ma30 < a.ma60 and a.ma60 < a.ma120 and b.pe_ttm < 40 and b.pb <10 and b.turnover_rate_f < 1.5 and a.trade_date = '" + date + "' and a.ts_code < '680000' order by 1"
    PRO_DBL = "SELECT a.ts_code FROM t_pro_daily a left join t_pro_dailybasic b on a.ts_code = b.ts_code and a.trade_date =b.trade_date where a.ma10 <a.ma30 and a.ma30 < a.ma60 and a.ma60 < a.ma120 and a.trade_date = '" + date + "' and b.pe_ttm < 40 and b.pb <10 and b.turnover_rate_f < 1.5 and a.ts_code < '680000' order by 1"
    for code in hq._excutesql(PRO_DBL).fetchall():
        #print(code)
        df =pd.DataFrame(hq._excutesql("select trade_date,high,low,close from t_pro_daily where ts_code = '" +code[0]+"' and trade_date <= '"+date+"' order by trade_date desc LIMIT 120").fetchall())
        df.columns=['date','high','low','close']
        df['code'] =code[0]
        df = df.reindex(index=df.index[::-1])
        df['diff'], df['dea'], df['macd'] = MACD(df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
        df['junction'] = df['diff'] / df['dea']
        low_min = df.groupby(['code'])['low'].min()
        # diff_min = df.groupby(['code'])['diff'].min()
        # diff = df[df['date'] == date]['diff']
        # #print(df[(df['junction'] < 0.99) & (df['junction'] > 0.95 )& (df['diff'] <0 ) & (df['dea']<0) & (df['macd']<0)])
        # #if(df[df['low']==low_min[0]]['date'].index[0]==0 and float(c.FORMAT(diff_min))/float(c.FORMAT(diff[0]))!=1):
        # if (df[(df['junction'] < 0.99) & (df['junction'] > 0.95 )& (df['diff'] <0 ) & (df['dea']<0) & (df['macd']>0)].index.min() < df[df['low'] == low_min[0]].index[0]):
        df_tmp = df[(df['junction'] < 0.99) & (df['junction'] > 0.95)]
        df_tmp['diff_v1'] = df_tmp['diff'] - df_tmp['diff'].shift(1)
        df_tmp['dea_v1'] = df_tmp['dea'] - df_tmp['dea'].shift(1)
        df_tmp['low_v1'] = df_tmp['low'] - df_tmp['low'].shift(1)
        df_tmp['diff_v2'] = df_tmp['diff'].shift(1) - df_tmp['diff'].shift(2)
        df_tmp['dea_v2'] = df_tmp['dea'].shift(1) - df_tmp['dea'].shift(2)
        df_tmp['low_v2'] = df_tmp['low'].shift(1) - df_tmp['low'].shift(2)
        df_tmp['low_v1'] = df_tmp['low'] - df_tmp['low'].shift(1)
        df_tmp['low_v2'] = df_tmp['low'].shift(1) - df_tmp['low'].shift(2)
        df_final = df_tmp[(df_tmp['diff_v1'] > 0) & (df_tmp['dea_v1'] > 0) & (df_tmp['low_v1'] < 0) & (df_tmp['diff_v2'] > 0) & (df_tmp['dea_v2'] > 0) & (df_tmp['low_v2'] < 0) & (df_tmp['low_v1']<0) & (df_tmp['low_v2']<0)]
        # if(df[(df['junction']<0.99) & (df['junction']>0.95)].index.min()<df[df['low']==low_min[0]].index[0]):
        #     print('1')
        if (len(df_final)>0):
            print(code)
            #print(df_final)


# def get_dbl(date):
#     df =pd.DataFrame(hq._excutesql("select trade_date,high,low,close from t_pro_daily where ts_code = '601800' order by trade_date desc LIMIT 120").fetchall())
#     df.columns=['date','high','low','close']
#     df['code'] = '000830'
#     df=df.reindex(index=df.index[::-1])
#     df['diff'], df['dea'], df['macd'] = MACD(df['close'].values,fastperiod=12, slowperiod=26, signalperiod=9)
#     df['junction']=df['diff']/df['dea']
#     low_min = df.groupby(['code'])['low'].min()
#     diff_min = df.groupby(['code'])['diff'].min()
#     df_tmp = df[(df['junction']<0.99) & (df['junction']>0.95)]
#     df_tmp['diff_v1'] = df_tmp['diff'] - df_tmp['diff'].shift(1)
#     df_tmp['dea_v1'] = df_tmp['dea'] - df_tmp['dea'].shift(1)
#     df_tmp['low_v1'] = df_tmp['low'] - df_tmp['low'].shift(1)
#     df_tmp['diff_v2'] = df_tmp['diff'].shift(1) - df_tmp['diff'].shift(2)
#     df_tmp['dea_v2'] = df_tmp['dea'].shift(1) - df_tmp['dea'].shift(2)
#     df_tmp['low_v2'] = df_tmp['low'].shift(1) - df_tmp['low'].shift(2)
#     df_tmp['low_v2'] = df_tmp['low'].shift(1) - df_tmp['low'].shift(2)
#     df_tmp['low_v1'] = df_tmp['low'] - df_tmp['low'].shift(1)
#     df_tmp['low_v2'] = df_tmp['low'].shift(1) - df_tmp['low'].shift(2)
#     print(df)
#     df_final = df_tmp[(df_tmp['diff_v1'] > 0) & (df_tmp['dea_v1'] > 0) & (df_tmp['low_v1'] < 0) & (df_tmp['diff_v2'] > 0) & (df_tmp['dea_v2'] > 0) & (df_tmp['low_v2'] < 0) & (df_tmp['low_v1']<0) & (df_tmp['low_v2']<0)]
#     print(df_tmp)
#     # if(df_tmp[(df_tmp['diff_v']>0) & (df_tmp['dea_v']>0) & (df_tmp['low_v']<0)].index.min()<df[df['low']==low_min[0]].index[0]):
#     #     print(1)