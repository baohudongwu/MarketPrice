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

#期权成交数据
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
    df.drop(['序号'],axis=1,inplace=True)
    print(df)
    df.columns = ['date', 'time', 'market', 'code', 'name', 'deal', 'type', 'price', 'quantity', 'amount','reserve_flag', 'serial', 'remark','fee']
    df['date'] = df['date'].map(FORMAT_date)
    df.to_sql('t_opt_entrust', c.ENGINE, if_exists='append')
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

def formatcode(x):
    if len(str(x))==2:
        x='0000'+str(x)
    if len(str(x))==3:
        x='000'+str(x)
    if len(str(x))==4:
        x='00'+str(x)
    return(x)

#同花顺对账单
#type dzd ,jgd

def importstockentrust(filename,type):
    excel_path = "c:\\"+filename+".xlsx"
    df = pd.read_excel(excel_path, sheet_name=filename)
    table = 'none'
    if (type == 'dzd'):
        table = 't_stock_dzd'
        #df.columns = ['date', 'series', 'code', 'name', 'operation', 'occ_amount', 'balance', 'remark', 'quantity','average_price', 'done_amount', 'done_series', 'market', 'stockholder', 'exchange_rate', 'fee_gh','fee_yh', 'fee_qt']
        df.columns = ['date', 'series', 'code', 'name', 'operation', 'occ_amount', 'balance', 'remark', 'quantity','average_price', 'done_amount', 'done_series', 'market', 'exchange_rate', 'fee_gh','fee_yh', 'fee_qt']
        df['name'] = df['remark'].map(c.STOCK_NAME)
        df['code'] = df['code'].apply(formatcode)
        df['series']=''
        df['market']=''
        #print(df)
    if (type=='jgd'):
        table = 't_stock_jgd'
        #df.columns=['date','code', 'name', 'operation','quantity','average_price', 'done_amount', 'balance', 'occ_amount', 'fee','fee_yh','fee_qt','amount','series','stockholder','fee_gh','jybz','jsbz','exchange_rate','hkfee']
        df.columns = ['date', 'code', 'name', 'operation', 'quantity', 'average_price', 'done_amount', 'balance',
                      'occ_amount', 'fee', 'fee_yh', 'fee_qt', 'amount', 'series', 'fee_gh', 'jybz',
                      'jsbz', 'exchange_rate', 'hkfee']
        df['code'] = df['code'].apply(formatcode)
        #广发交割单
    if (type=='gfjgd'):
        table = 't_stock_gfjgd'
        df.columns=['date','code', 'name', 'operation','quantity','average_price', 'done_amount', 'balance','us_balance', 'occ_amount','fee_yh','hth','amount','fee_gh','fee_jingshou','fee_zhengguan','fee_yj','fee_jiesuan']
        df['code'] = df['code'].apply(formatcode)
        #print(df)
    df.to_sql(table, c.ENGINE, if_exists='append')


#通达信数据导入（市盈(动)，	地区，	市盈(TTM)，	市盈(静)	，市净率，	股息率%，	贝塔系数，	细分行业）
def importTDXHY(filename):
    hq._excutesql("truncate table T_TDXDATA")
    excel_path = "C:\\"+filename+".xlsx"
    df = pd.read_excel(excel_path, sheet_name=filename)
    df.drop(df.columns[[2,3,4]], axis=1, inplace=True)
    df.drop([len(df) - 1], inplace=True)
    df.columns = ['code','name','hy','sz','pe','ped','pettm','pb','gx','beta','dq']
    df['code'] = df['code'].apply(lambda x: str(x)).str.pad(6, side='left', fillchar='0')
    df.to_sql("T_TDXDATA", c.ENGINE, if_exists='append')
    hq._excutesql("update t_tdxdata set sz = SUBSTR(TRIM(sz), 1 , LENGTH(TRIM(sz))-3)")

#通达信概念，通达信->选项->导出数据->板块导出
def importTDXGN(filename):
    hq._excutesql("truncate table T_TDXDATA_GN")
    excel_path = "C:\\"+filename+".txt"
    df = pd.read_csv(excel_path,sep=';',encoding='gbk')
    df.columns = ['gn_code','gn_name','code','name']
    df['code']=df['code'].apply(lambda x: str(x)).str.pad(6,side='left',fillchar='0')
    df.to_sql("T_TDXDATA_GN", c.ENGINE, if_exists='append')

#通达信换手率
def importTDXHSL(filename,date):
    print("start to import ")
    excel_path = "C:\\"+filename+".xlsx"
    df = pd.read_excel(excel_path, sheet_name=filename)
    df.drop(df.columns[[1,2,3,5,6,7,8,9,10,11,12,13]], axis=1, inplace=True)
    df.drop([len(df)-1],inplace=True)
    df.columns = ['code','turnrate']
    df['code']=df['code'].apply(lambda x: str(x)).str.pad(6,side='left',fillchar='0')
    df['date']=date
    df.to_sql("T_turnrate", c.ENGINE, if_exists='append')
    os.remove(excel_path)
    print("OVER")



#财汇换手率
#期权成交数据
def importCH(filename):
    csv_path = u'D:\\turn2\\' + filename + '.csv'
    df=pd.read_csv(csv_path, encoding='gb18030')
    df.columns = ['date', 'code', 'turnrate']
    df['code'] = df['code'].apply(formatcode)
    print(df)
    df.to_sql("T_turnrate", c.ENGINE, if_exists='append')import pandas as pd
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

#期权成交数据
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
    df.drop(['序号'],axis=1,inplace=True)
    print(df)
    df.columns = ['date', 'time', 'market', 'code', 'name', 'deal', 'type', 'price', 'quantity', 'amount','reserve_flag', 'serial', 'remark','fee']
    df['date'] = df['date'].map(FORMAT_date)
    df.to_sql('t_opt_entrust', c.ENGINE, if_exists='append')
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

def formatcode(x):
    if len(str(x))==2:
        x='0000'+str(x)
    if len(str(x))==3:
        x='000'+str(x)
    if len(str(x))==4:
        x='00'+str(x)
    return(x)

#同花顺对账单
#type dzd ,jgd

def importstockentrust(filename,type):
    excel_path = "c:\\"+filename+".xlsx"
    df = pd.read_excel(excel_path, sheet_name=filename)
    table = 'none'
    if (type == 'dzd'):
        table = 't_stock_dzd'
        #df.columns = ['date', 'series', 'code', 'name', 'operation', 'occ_amount', 'balance', 'remark', 'quantity','average_price', 'done_amount', 'done_series', 'market', 'stockholder', 'exchange_rate', 'fee_gh','fee_yh', 'fee_qt']
        df.columns = ['date', 'series', 'code', 'name', 'operation', 'occ_amount', 'balance', 'remark', 'quantity','average_price', 'done_amount', 'done_series', 'market', 'exchange_rate', 'fee_gh','fee_yh', 'fee_qt']
        df['name'] = df['remark'].map(c.STOCK_NAME)
        df['code'] = df['code'].apply(formatcode)
        df['series']=''
        df['market']=''
        #print(df)
    if (type=='jgd'):
        table = 't_stock_jgd'
        #df.columns=['date','code', 'name', 'operation','quantity','average_price', 'done_amount', 'balance', 'occ_amount', 'fee','fee_yh','fee_qt','amount','series','stockholder','fee_gh','jybz','jsbz','exchange_rate','hkfee']
        df.columns = ['date', 'code', 'name', 'operation', 'quantity', 'average_price', 'done_amount', 'balance',
                      'occ_amount', 'fee', 'fee_yh', 'fee_qt', 'amount', 'series', 'fee_gh', 'jybz',
                      'jsbz', 'exchange_rate', 'hkfee']
        df['code'] = df['code'].apply(formatcode)
        #广发交割单
    if (type=='gfjgd'):
        table = 't_stock_gfjgd'
        df.columns=['date','code', 'name', 'operation','quantity','average_price', 'done_amount', 'balance','us_balance', 'occ_amount','fee_yh','hth','amount','fee_gh','fee_jingshou','fee_zhengguan','fee_yj','fee_jiesuan']
        df['code'] = df['code'].apply(formatcode)
        #print(df)
    df.to_sql(table, c.ENGINE, if_exists='append')


#通达信数据导入（市盈(动)，	地区，	市盈(TTM)，	市盈(静)	，市净率，	股息率%，	贝塔系数，	细分行业）
def importTDXHY(filename):
    hq._excutesql("truncate table T_TDXDATA")
    excel_path = "C:\\"+filename+".xlsx"
    df = pd.read_excel(excel_path, sheet_name=filename)
    df.drop(df.columns[[2,3,4]], axis=1, inplace=True)
    df.drop([len(df) - 1], inplace=True)
    df.columns = ['code','name','hy','sz','pe','ped','pettm','pb','gx','beta','dq']
    df['code'] = df['code'].apply(lambda x: str(x)).str.pad(6, side='left', fillchar='0')
    df.to_sql("T_TDXDATA", c.ENGINE, if_exists='append')
    hq._excutesql("update t_tdxdata set sz = SUBSTR(TRIM(sz), 1 , LENGTH(TRIM(sz))-3)")

#通达信概念，通达信->选项->导出数据->板块导出
def importTDXGN(filename):
    hq._excutesql("truncate table T_TDXDATA_GN")
    excel_path = "C:\\"+filename+".txt"
    df = pd.read_csv(excel_path,sep=';',encoding='gbk')
    df.columns = ['gn_code','gn_name','code','name']
    df['code']=df['code'].apply(lambda x: str(x)).str.pad(6,side='left',fillchar='0')
    df.to_sql("T_TDXDATA_GN", c.ENGINE, if_exists='append')

#通达信换手率
def importTDXHSL(filename,date):
    print("start to import ")
    excel_path = "C:\\"+filename+".xlsx"
    df = pd.read_excel(excel_path, sheet_name=filename)
    df.drop(df.columns[[1,2,3,5,6,7,8,9,10,11,12,13]], axis=1, inplace=True)
    df.drop([len(df)-1],inplace=True)
    df.columns = ['code','turnrate']
    df['code']=df['code'].apply(lambda x: str(x)).str.pad(6,side='left',fillchar='0')
    df['date']=date
    df.to_sql("T_turnrate", c.ENGINE, if_exists='append')
    os.remove(excel_path)
    print("OVER")



#财汇换手率
#期权成交数据
def importCH(filename):
    csv_path = u'D:\\turn2\\' + filename + '.csv'
    df=pd.read_csv(csv_path, encoding='gb18030')
    df.columns = ['date', 'code', 'turnrate']
    df['code'] = df['code'].apply(formatcode)
    print(df)
    df.to_sql("T_turnrate", c.ENGINE, if_exists='append')
