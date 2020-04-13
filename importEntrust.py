import pandas as pd
import time
import datetime
from pandas import DataFrame
import common as c
import os
import hq as hq

FORMAT_date = lambda x: x.replace('-','')
FORMAT_SPACE = lambda x: x.strip()
DATE = time.strftime('%Y%m%d',time.localtime())
DATE = DATE[:4]+'年'+str(int(DATE[4:6]))+'月'+str(int(DATE[6:8]))+'日'
#DATE = '180812'
#print(DATE)
#path = "C:/image/" + DATE + "/"

def csv_to_excle():
    if (os.path.exists(csv_path)):
        csv = pd.read_csv(csv_path, encoding='gb18030')
        csv.to_excel(excel_path, sheet_name=DATE)
    else:
        csv = pd.read_csv("c:\\risk.csv", encoding='gb18030')
        csv.to_excel("c:\\risk.xlsx", sheet_name='risk')

#成交数据
def importentrust(date):
    csv_path = u'C:\\' + DATE + '.csv'
    excel_path = u'C:\\' + DATE + '.xlsx'
    sql = "delete from t_entrust where date = '"+date+"'"
    hq._excutesql(sql)
    #df = pd.read_csv(excel_path, index_col=False, quoting=3, sep=" ",encoding='gb18030')
    #csv_to_excle()
    pd.read_csv(csv_path, encoding='gb18030').to_excel(excel_path, sheet_name=DATE) #csv转excle
    df = pd.read_excel(excel_path,sheet_name=DATE)
    df.drop(df.columns[len(df.columns) - 1], axis=1, inplace=True)
    df.columns = ['date', 'time', 'market', 'code', 'name', 'deal', 'type', 'price', 'quantity', 'amount','reserve_flag', 'serial', 'remark','fee']
    df['date'] = df['date'].map(FORMAT_date)
    df.to_sql('t_entrust', c.ENGINE, if_exists='append')
    os.remove(excel_path)
    os.remove(csv_path)

#期权风险指标--未用
def importrisk(date):
    csv_path = u'C:\\risk.csv'
    sql = "delete from t_opt_risk where date = '" + date + "'"
    hq._excutesql(sql)
    df = pd.read_csv(csv_path,encoding='gb18030')
    df.drop(df.columns[len(df.columns) - 1], axis=1, inplace=True)
    df.reset_index(level=0, inplace=True)
    df['index'] =df['index'].map(FORMAT_date)
    df.columns = ['date', 'ts_code', 'opt_code', 'name', 'Delta', 'Theta', 'Gamma', 'Vega', 'Rho']
    df['date'] = df['date'].map(FORMAT_SPACE)
    df['opt_code'] = df['opt_code'].map(FORMAT_SPACE)
    df['name'] = df['name'].map(FORMAT_SPACE)
    df.to_sql('t_opt_risk', c.ENGINE, if_exists='append')
    os.remove(csv_path)

#format_column之间分隔用!format_column与format格式之间用#
#先源路径，后生成路径
def importcsv_excle(date):
    sql = "select function_name,file_path,sql_table_name,columns_name,format_columns,csv_excel from t_importfuction"
    for fn,fp,sn,cn,fc,isexcel in hq._excutesql(sql):
        csv_path = eval(fp.split('!')[0])
        if (os.path.exists(csv_path)):
            print(fn)
            sql_d = "delete from " + sn + " where date = '" + date + "'"
            hq._excutesql(sql_d)
            if (isexcel=='y'):
                excel_path = eval(fp.split('!')[1])
                pd.read_csv(csv_path, encoding='gb18030').to_excel(excel_path, sheet_name=DATE)  # csv转excle
                df = pd.read_excel(excel_path, sheet_name=DATE)
                os.remove(excel_path)
            else:
                df = pd.read_csv(csv_path, encoding='gb18030')
                df.reset_index(level=0, inplace=True)
            df.drop(df.columns[len(df.columns) - 1], axis=1, inplace=True)
            if (fn=='importentrust'):
                df.drop(df.columns[0], axis=1, inplace=True)
            #print(df)
            df.columns = eval(cn)
            if len(fc)>0:#整理数据格式
                for i in range(len(fc.split('!'))):
                    df[fc.split('!')[i].split('#')[0]] = df[fc.split('!')[i].split('#')[0]].map(eval(fc.split('!')[i].split('#')[1]))
            df.to_sql(sn, c.ENGINE, if_exists='append')
            os.remove(csv_path)
        else:
            print("No file "+csv_path)

def importmonthlystatement(filename):
    excel_path = "c:\\"+filename+".xlsx"
    df = pd.read_excel(excel_path, sheet_name=filename)
    df.columns = ['date', 'series', 'code', 'name', 'operation', 'occ_amount', 'balance', 'remark', 'quantity',
                  'average_price', 'done_amount', 'done_series', 'market', 'stockholder', 'exchange_rate', 'fee_gh',
                  'fee_yh', 'fee_qt']
    df.to_sql('t_monthlystatement', c.ENGINE, if_exists='append')
