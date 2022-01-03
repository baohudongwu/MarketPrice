import numpy as np
import pandas as pd
import matplotlib as mp
import common as c
import hq as hq


def lookup(remark):
    df = pd.DataFrame(hq._excutesql(c.SQL_RECORD).fetchall())
    df.columns = ['date', 'code','type','remark']
    dfdate=pd.DataFrame(hq._excutesql(c.SQL_TRADEDAY).fetchall())
    count=0
    count_e=0
    list =[]
    list1=[]
    for i in range(0,len(df)):
        str = "select date,code,high,low,close from t_hisdata_all where code ='"+df['code'][i]+"' and date >'"+df['date'][i]+"' order by date,code"
        dfs = pd.DataFrame(hq._excutesql(str).fetchall())
        if (dfs.empty):
            continue
        dfs.columns = ['date', 'code', 'high', 'low', 'close']
        max = dfs.groupby(['code'])['close'].max()[0]
        tmp = dfs['date'][np.where(dfs['close']==max)[0]].to_string()
        date_max = tmp[len(tmp)-10:len(tmp)]
        if (int(hq.get_days(df['date'][i],date_max,dfdate))>1):#最大值日期需T+2日以上
            count= count+1
            list.append([df['date'][i],date_max,df['code'][i],dfs['close'][0],max,(max-dfs['close'][0])/dfs['close'][0]*100,df['type'][i],df['remark'][i],1])
        else:
            count_e=count_e+1
            list.append([df['date'][i], date_max, df['code'][i], dfs['close'][0], dfs['close'][len(dfs)-1], (dfs['close'][len(dfs)-1] - dfs['close'][0]) / dfs['close'][0] * 100,df['type'][i], df['remark'][i], 0])
    list.append([count,(count+count_e),count/(count+count_e)*100])
    for j in list:
        if (len(j)==9):
            list1.append(j)
    print(list)
    hq._excutesql("delete from t_lookup")
    df1=pd.DataFrame(list1)
    df1.columns = ['fdate', 'sdate','code', 'bprice', 'sprice', 'aoi','type','parameter','flag']
    df1.insert(9,'remark',remark)
    df1.to_sql("t_lookup", c.ENGINE, if_exists='append')

'''
比较最高价和最低价的日期，计算胜率
'''
def cal(list,date):
    sql = "select ts_code,trade_date,`close`,high,low from t_pro_daily where ts_code in ("+str(list).replace('[','').replace(']','')+") and trade_date >= '"+date+"'"
    df = pd.DataFrame(hq._excutesql(sql).fetchall())
    df.columns = ['code', 'date', 'close', 'high','low']
    df_max = df.groupby(['code'])['high'].max()
    df_min = df.groupby(['code'])['low'].min()
    # max_list =[]
    # min_list=[]
    # max_pd = pd.DataFrame(columns=['code','dfindex','date','close','flag'])
    # min_pd = pd.DataFrame(columns=['code', 'dfindex', 'date','close','flag'])
    max_pd = pd.DataFrame(columns=['code', 'date', 'close', 'flag'])
    min_pd = pd.DataFrame(columns=['code', 'date', 'close', 'flag'])
    for code in df_max.index:
        i = np.where((df['code'] == code) & (df['high'] == df_max[code]))[0][0]
        # max_list.append(code)
        # max_list.append(i)
        # max_list.append(df.iloc[i]['date'])
       # max_pd = max_pd.append({'code':code,'dfindex':str(i),'date':df.iloc[i]['date'],'close':df.iloc[i]['close'],'flag':'max'},ignore_index=True)
        max_pd = max_pd.append({'code': code, 'date': df.iloc[i]['date'], 'close': df.iloc[i]['close'], 'flag': 'max'},ignore_index=True)
    for code in df_min.index:
        #print(code)
        i = np.where((df['code'] == code) & (df['low'] == df_min[code]))[0][0]
        # min_list.append(code)
        # min_list.append(i)
        # min_list.append(df.iloc[i]['date'])
        #min_pd = min_pd.append({'code': code, 'dfindex': str(i), 'date': df.iloc[i]['date'],'close':df.iloc[i]['close'],'flag':'min'}, ignore_index=True)
        min_pd = min_pd.append({'code': code, 'date': df.iloc[i]['date'], 'close': df.iloc[i]['close'], 'flag': 'min'},ignore_index=True)
    #df_all = pd.concat([max_pd, min_pd], axis=0, ignore_index=True)
    df_base = df[df['date']==date].drop(['high','low'], axis=1)
    df_base.insert(3, 'flag', 'base')
    df_tmp = pd.merge(max_pd, min_pd,on=['code'])
    df_all = pd.merge(df_tmp, df_base,on=['code'])
    df_all['天数'] = df_all['date_x'].astype(int)-df_all['date'].astype(int)
    df_all['收益率'] = (df_all['close_x']/df_all['close']-1).map(lambda x:round(x,2))*100
    df_all['亏损率'] = (df_all['close_y']/df_all['close']-1).map(lambda x:round(x,2))*100
    df_all['date'] = pd.to_datetime(df_all['date'])
    df_all['date_x'] = pd.to_datetime(df_all['date_x'])
    df_all['date_y'] = pd.to_datetime(df_all['date_y'])
    #df_all['min_day'] =pd.DataFrame(df_all['date_y']-df_all['date'])
    # print(df_all['date_y'])
    # print(df_all['date'])
    #filename = r"C:\Users\Administrator\Desktop\\"+ date +".xlsx"
    filename = r"E:\huice\\"+ date +".xlsx"
    df_all.to_excel(filename,sheet_name='passengers',index=False)
