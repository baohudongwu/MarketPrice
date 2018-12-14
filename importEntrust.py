import pandas as pd
import time
import datetime
from pandas import DataFrame
import common as c
import os
import hq as hq

FORMAT_date = lambda x: x.replace('-','')
DATE = time.strftime('%Y%m%d',time.localtime())
DATE = DATE[:4]+'年'+DATE[4:6]+'月'+DATE[6:8]+'日'
csv_path =u'C:\\'+DATE+'.csv'
excel_path = u'C:\\'+DATE+'.xlsx'
#DATE = '180812'
#print(DATE)
#path = "C:/image/" + DATE + "/"

def csv_to_excle():
    csv = pd.read_csv(csv_path, encoding='gb18030')
    csv.to_excel(excel_path, sheet_name=DATE)

def importexcel(date):
    sql = "delete from t_entrust where date = '"+date+"'"
    hq._excutesql(sql)
    #df = pd.read_csv(excel_path, index_col=False, quoting=3, sep=" ",encoding='gb18030')
    csv_to_excle()
    df = pd.read_excel(excel_path,sheet_name=DATE)
    df.drop(df.columns[len(df.columns) - 1], axis=1, inplace=True)
    df.columns = ['date', 'time', 'market', 'code', 'name', 'deal', 'type', 'price', 'quantity', 'amount','reserve_flag', 'serial', 'remark','fee']
    df['date'] = df['date'].map(FORMAT_date)
    df.to_sql('t_entrust', c.ENGINE, if_exists='append')
    os.remove(excel_path)