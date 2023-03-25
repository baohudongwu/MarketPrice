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
    #PRO_DBL = "SELECT a.ts_code FROM t_pro_daily a left join t_pro_dailybasic b on a.ts_code = b.ts_code and a.trade_date =b.trade_date where a.ma10 <a.ma30 and a.ma30 < a.ma60 and a.ma60 < a.ma120 and a.trade_date = '" + date + "' and b.turnover_rate_f < 1.5 and a.ts_code < '680000' and pb is not null order by 1"
    PRO_DBL = "SELECT a.ts_code FROM t_pro_daily a where a.ma10 <a.ma30 and a.ma30 < a.ma60 and a.ma60 < a.ma120 and a.trade_date =  '" + date + "'  and a.ts_code < '680000'  order by 1"
    #print(PRO_DBL)
    list = []
    for code in hq._excutesql(PRO_DBL).fetchall():
        #print(code)
        df =pd.DataFrame(hq._excutesql("select trade_date,high,low,close from t_pro_daily where ts_code = '" +code[0]+"' and trade_date <= '"+date+"' order by trade_date desc LIMIT 120").fetchall())
        df.columns=['date','high','low','close']
        df['code'] =code[0]
        df = df.reindex(index=df.index[::-1])
        df['diff'], df['dea'], df['macd'] = MACD(df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
        df['junction'] = df['diff'] / df['dea']
        low_min = df.groupby(['code'])['low'].min()
        df['diff_v'] = df['diff'] - df['diff'].shift(1)
        df_tmp = df[(df['junction'] < 0.99) & (df['junction'] > 0.95) & (df['diff_v'] > 0)]
        df_tmp['diff_v1'] = df_tmp['diff'] - df_tmp['diff'].shift(1)
        df_tmp['dea_v1'] = df_tmp['dea'] - df_tmp['dea'].shift(1)
        df_tmp['low_v1'] = df_tmp['low'] - df_tmp['low'].shift(1)
        df_tmp['diff_v2'] = df_tmp['diff'].shift(1) - df_tmp['diff'].shift(2)
        df_tmp['dea_v2'] = df_tmp['dea'].shift(1) - df_tmp['dea'].shift(2)
        df_tmp['low_v2'] = df_tmp['low'].shift(1) - df_tmp['low'].shift(2)
        df_tmp['low_v1'] = df_tmp['low'] - df_tmp['low'].shift(1)
        df_tmp['low_v2'] = df_tmp['low'].shift(1) - df_tmp['low'].shift(2)
        df_final = df_tmp[(df_tmp['diff_v1'] > 0) & (df_tmp['dea_v1'] > 0) & (df_tmp['low_v1'] < 0) & (df_tmp['diff_v2'] > 0) & (df_tmp['dea_v2'] > 0) & (df_tmp['low_v2'] < 0) & (df_tmp['low_v1']<0) & (df_tmp['low_v2']<0)]
        if (len(df_final)>0):
            list.append(code[0])
    print(list)
    return list

''' 反包
1、五日线高于55日线
2、最新四天数据，基准日为最新日期(第四天),基准日为跌(开盘价大于收盘价)
3、第一天为涨
4、后三天为跌势，成交量第二天最高，第三天小于第二天80%，基准日成交量最小
5、没有顶背离
'''
def get_fb(date):
    #PRO_FB = "select a.ts_code from t_pro_daily a,t_tdxdata b where a.ma5>a.ma10 and a.ma10 >a.ma20 and a.ma20 > a.ma30 and a.ma30 > a.ma60 and a.ma60 > a.ma120 and a.trade_date = '" + date.replace('-','') + "' and a.close<a.open and (b.pettm <>'--' or b.pe<>'--') and a.ts_code = b.code"# and a.close <= a.pre_close"
    #PRO_FB = "select a.ts_code from t_pro_daily a,t_tdxdata b where a.ma20 > a.ma30 and a.ma30 > a.ma60 and a.ma60 > a.ma120 and a.trade_date = '" + date.replace('-', '') + "' and a.close<a.open and (b.pettm <>'--' or b.pe<>'--') and a.ts_code = b.code and a.close <= a.pre_close"
    #PRO_FB = "select a.ts_code from t_pro_daily a where a.ma60 > a.ma120 and a.trade_date = '" + date.replace('-', '') + "'  and a.close<a.open"  # and a.close <= a.pre_close"
    PRO_FB = "select a.ts_code from t_pro_daily a,t_tdxdata b where a.ma60 > a.ma120 and a.trade_date = '" + date.replace('-', '')+ "' and a.close<a.open and (b.pettm <>'--' or b.pe<>'--') and a.ts_code = b.code" #剔除pe为负的
    #PRO_FB = "select a.ts_code from t_pro_daily a where a.trade_date = '" + date.replace('-','') + "'  and a.close<=a.open"  # and a.close <= a.pre_close"
    #PRO_FB = "select DISTINCT code from t_limit_detail where  date <='"+date.replace('-','')+"' and date > '"+hq.get_Xtradedate(date,8).replace('-','')+"' and code in (select ts_code from t_pro_daily a where a.ma5>a.ma10 and a.ma10 >a.ma20 and a.ma20 > a.ma30 and a.ma30 > a.ma60 and a.ma60 > a.ma120 and trade_date = '"+date.replace('-','')+"' and close <= open ) order by 1" #8个交易日内有涨停
    #PRO_FB = "select DISTINCT code from t_limit_detail where  top = 0 and date <='" + date.replace('-','') + "' and date > '" + hq.get_Xtradedate(date, 8).replace('-', '') + "' and code in (select ts_code from t_pro_daily where trade_date = '" + date.replace('-', '') + "' and close <= open ) order by 1"  # 8个交易日内有涨停
    print(PRO_FB)
    list = []
    for code in hq._excutesql(PRO_FB).fetchall():
        #print(code)
        try:
            df = pd.DataFrame(hq._excutesql("select ts_code,trade_date,open,close,pre_close,amount,high from t_pro_daily where ts_code =  '" +code[0]+"' and trade_date <= '"+date.replace('-','')+"' order by trade_date desc limit 4").fetchall())
            #df = pd.DataFrame(hq._excutesql("select ts_code,trade_date,open,close,pre_close,vol,high from t_pro_daily where ts_code = 300619 and trade_date <=20210812 order by trade_date desc limit 4").fetchall())
            df.columns = ['code','date', 'open','close', 'pre_close', 'vol','high']
            df_tmp = df.drop(labels=3)  # 下跌三天
            df_min_vol = df_tmp.groupby(['code'])['vol'].min()
            df_max_vol = df_tmp.groupby(['code'])['vol'].max()
            #print(df)
            try:
                if(df[df['vol']==df_min_vol[0]]['date'][0]==date.replace('-','')): #基准日是最小量
                    # print(df['code'][0])
                    # print('111111111111111111111111')
                    if(df['vol'][2]==df_max_vol[0]):   #第二天成交量最高
                        # print(df['code'][0])
                        # print('22222222222222222')
                        if(df['open'][3]<=df['close'][3]): #第一天是红的
                            # print(df['code'][0])
                            # print('33333333333333333333')
                            if(df['open'][2]>df['close'][2]):
                                # print(df['code'][0])
                                # print('4444444444444444444444444')
                                if(df['open'][1] > df['close'][1]):
                                    list.append(code[0])
                                    #print(df['code'][0])
                                    #print(code[0])
                    #print(code[0])


            except Exception as e:
                continue
        except Exception as e:
            continue
    print(list)
    return list

'''1、连板2天的
   2、连板3天的
'''
def getLB(days):
    LB_SQL = "SELECT date,code from v_limitup_detail where date > '20220101' and name not like '%%ST%%'"
    df = pd.DataFrame(hq._excutesql(LB_SQL).fetchall())
    df.columns = ['date','code']
    df_grp = df.groupby(['code'])
    for date,code in df_grp:
        print(code)
        print(date)

#计算MACD
def getMACD(date):
    pro_code_sql = "select ts_code from t_pro_daily where trade_date = '"+date+"' and ts_code <'000002'"
    for code in hq._excutesql(pro_code_sql).fetchall():
        print(code[0])
        df = pd.DataFrame(hq._excutesql("select trade_date,close from t_pro_daily where ts_code = '" + code[
            0] + "' and trade_date <= '" + date + "' order by trade_date desc LIMIT 126").fetchall())
        df.columns = ['date', 'close']
        df['code'] = code[0]
        df = df.reindex(index=df.index[::-1])
        df['diff'], df['dea'], df['macd'] = MACD(df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
        df=df.drop(['close'], axis=1)
        df['diff']=df['diff'].map(lambda x:round(x,4))
        df['dea'] = df['dea'].map(lambda x: round(x, 4))
        df['macd'] = df['macd'].map(lambda x: round(x, 4))
        df=df[df['date']==date]
        print(df)
        #df.ix[:0].to_sql('t_kline', c.ENGINE, if_exists='append')
        df.to_sql('t_macd', c.ENGINE, if_exists='append')

'''
均线支撑：
两种情况：
1、上升通道
2、下降通道
'''

'''低吸
--5日线依次大于10,20,30（是否限制几条线之间的距离）
--最低价大于5日线
--收盘价上升
--不能是近期高点
--MACD不能粘合以及须向上，大于0轴
--不能顶背离
--要在中高位
--次日最高价要超过5日线，注意不能是钓鱼线
--注意成交量
---需要确定周期

'''
