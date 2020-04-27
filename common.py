from sqlalchemy import create_engine
import time
import hq as hq

#ENGINE = create_engine('mysql://root:1@10.16.90.119/hq?charset=utf8')
ENGINE = create_engine('mysql://root:root@127.0.0.1/hq?charset=utf8')
#ENGINE = create_engine('mysql://root:root@114.116.65.109/hq?charset=utf8')
FORMAT = lambda x: '%.2f' % x
#PRO_CODE_FOMART = lambda  x : x[0:6]
PRO_CODE_FORMAT = lambda  x: x.split('.')[0]
STOCK_NAME = lambda  x: x.split(':')[1]
MA  = [5,10,20,30,60,120]

#"2011-01-01":"2011-12-31","2012-01-01":"2012-12-31","2013-01-01":"2013-12-31",
#PERIOD={"2014-01-01":"2014-12-31","2015-01-01":"2015-12-31","2016-01-01":"2016-12-31","2017-01-01":"2017-12-31","2018-01-01":"2018-12-31"}
PERIOD={"2018-06-30":"2018-07-31"}
#SQL_TRADEDAY = "SELECT * FROM t_tradeday where isopen =1"
SQL_TRADEDAY = "SELECT * FROM t_pro_tradeday where is_open =1"
SQL_CODE = "select code from T_StockBasics order by code"
DATE = hq.istradeday(time.strftime('%Y-%m-%d',time.localtime()))
#DATE='2019-01-02'
#START=(datetime.datetime.now()+datetime.timedelta(days=-4)).strftime('%Y-%m-%d')
YEAR = 2018
QUARTER =2
VOL =  1.618
TURNOVER = 1.618
DOJI= 1
m5_10 = 1.006
m10_20 = 1.006
p_change = 5
DAYS=22
ROE=0
PB=0
PE=0
UPSD_RATIO=0.01
DOWNSD_RATIO=0.03
#SQL_LASTTRADEDAY = "SELECT max(calendardate) from t_tradeday where  isopen = 1 and calendardate <'"+DATE+"'"
SQL_LASTTRADEDAY = "SELECT max(cal_date) from t_pro_tradeday where  is_open = 1 and cal_date <'"+DATE+"'"
#BEFROE = hq.get_lasttradeday(DAYS)
BEFROE ='2018-12-01'
####################老接口##############################
SQL_LONG = "select code from t_hisdata_all where date = '"+DATE+"' and (ma5>ma10 and ma10>ma20) and (ma5/ma10>"+str(m5_10)+" and ma10/ma20>"+str(m10_20)+") and (v_ma5>v_ma10 and v_ma10>v_ma20 )"
#SQL_LONG = "select code from t_hisdata_all where date = '"+hq.istradeday(DATE)+"' and ma5>ma10 and close > ma5"
SQL_SHORT ="select code from t_hisdata_all where date = '"+DATE+"' and (ma5>ma10 and ma20>ma5)"
SQL_SOARED ="SELECT date,code,volume,turnover,open,close,high,low FROM t_hisdata_all WHERE date BETWEEN '"+BEFROE+"' AND '"+DATE+"'AND code in ("+SQL_LONG+") ORDER BY code,date"
SQL_FALL ="SELECT date,code,volume,turnover,open,close,high,low FROM t_hisdata_all WHERE date BETWEEN '"+BEFROE+"' AND '"+DATE+"'AND code in ("+SQL_SHORT+") ORDER BY code,date"
SQL_DOJI ="select date,code,volume,p_change from t_hisdata_all where code in (select code from t_hisdata_all where code in (select code from t_hisdata_all where date = '"+hq.get_lasttradeday(1)+"' and high > close and close > low and (ma5<ma10 and ma10<ma20)  and (ma10/ma5> "+str(m5_10)+" and ma20/ma10>"+str(m10_20)+")) and date ='"+hq.get_lasttradeday(2)+"' and ABS(p_change) > "+str(p_change)+ " and p_change < 0) and date BETWEEN '"+hq.get_lasttradeday(2)+"' and '"+DATE+"' order by code,date"
SQL_VSHRINK ="select date,code,volume,p_change from t_hisdata_all where date between '"+BEFROE+"' AND '"+DATE+"'and code in (select code from t_hisdata_all where date = '"+DATE+"' and ma5>1.0101*ma10 and ma10>1.0101*ma20) order by code,date"
# SQL_TMP = "select code from t_hisdata_all where date = '"+hq.istradeday(DATE)+"' and ma5>ma10"
# SQL_VSHRINK ="select date,code,volume,p_change from t_hisdata_all where date between '"+BEFROE+"' AND '"+DATE+"'AND code in ("+SQL_TMP+") ORDER BY code,date"
SQL_GRADIENT="select date,code,high,low from t_hisdata_all where date between '"+BEFROE+"' AND '"+DATE+"' and code in (select code from t_hisdata_all where date = '"+DATE+"' and ma5>ma10 and ma10> ma20) order by code"
START = '2018-04-27'
END =  '2018-05-27'
SQL_RECORD = "select date,code,type,remark from t_record where type <> 'get_aoi' and date between +'"+START+"' and '"+END+"' order by date,code"
SQL_PLATFORM_W = "select code from t_hisdata_all where close > ma5 and ma5>ma10 and ma5>ma20 and ma5/ma10> "+str(m5_10) + " and ma10/ma20 > "+str(m10_20)+" and date = '"+DATE+"'"
SQL_PLATFORM = "SELECT date,code,close,high,low,ma5,ma10,ma20,p_change,volume,turnover from t_hisdata_all WHERE date BETWEEN '"+BEFROE+"' AND '"+DATE+"'AND code in ("+SQL_PLATFORM_W+") ORDER BY code,date"
SQL_MONTH = "SELECT `index`, code, close, high, low, vol FROM t_monthdata where datetime >'2018-02-01' and LENGTH(vol)<20 order by code,datetime"
SQL_WEEK = "SELECT `index`, code, close, high, low, vol FROM t_weekdata where datetime >'2018-05-01' and LENGTH(vol)<20 order by code,datetime"
#############BAR################
BAR_LONG = "select code from t_daydata where datetime = '"+DATE+"' and (ma5>ma10 and ma10>ma20 and ma20>ma30) and (ma5/ma10>"+str(m5_10)+" and ma10/ma20>"+str(m10_20)+" and ma20/ma30>"+str(m10_20)+")"
BAR_SHORT ="select code from t_daydata where datetime = '"+DATE+"' and ma120>ma5"
BAR_SOARED ="SELECT datetime,code,vol,tor,open,close,high,low,amount,vr FROM t_daydata WHERE datetime BETWEEN '"+BEFROE+"' AND '"+DATE+"'AND code in ("+BAR_LONG+") ORDER BY code,datetime"
BAR_FALL ="SELECT datetime,code,vol,tor,open,close,high,low,amount,vr FROM t_daydata WHERE datetime BETWEEN '"+BEFROE+"' AND '"+DATE+"'AND code in ("+BAR_SHORT+") ORDER BY code,datetime"
##############PRO##################
PRO_STOCK_LIST = "SELECT ts_code FROM T_PRO_STOCKLIST WHERE LIST_STATUS = 'L'"