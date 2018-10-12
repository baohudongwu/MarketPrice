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
