import datetime
import common as c
import hq as hq
import matplotlib.pyplot as plt
import pandas as pd
import time
import os

def tor_vr(start,end,code,para):
    sql = "select datetime,close,tor,vr,vol from t_daydata where datetime between '"+start+"' and '"+end+"' and code = '"+code+"'"
    title = code+" ["+start+","+end+" ] "+str(para)
    datetime = []
    close = []
    tor = []
    vr = []
    vol = []
    for row in hq._excutesql(sql):
        datetime.append(row[0])
        close.append(row[1])
        tor.append(row[2])
        vr.append(row[3])
        vol.append(row[4]/100000)
    # 创建子图
    fig, ax = plt.subplots()
    #fig.subplots_adjust(bottom=0.2)
    plt.figure(1,figsize=(150, 130))
    plt.subplot(212)
    # 设置X轴刻度为日期时间
    ax.xaxis_date()
    plt.title(title)
    plt.xticks()#pd.date_range(start,end))
    plt.yticks()
    #plt.xlabel("BLACK close,YELLOW tor,GREEN vr,BLUE vol")
    plt.ylabel("")
    #plt.plot(datetime,close,color = 'black')
    plt.plot(datetime, tor,color = 'yellow')
    plt.plot(datetime, vr,color = 'red')
    plt.xlabel("YELLOW tor,RED vr")
    #plt.plot(datetime, vol, color='blue')
    plt.grid()

    plt.subplot(221)
    plt.plot(datetime, vol, color='blue')
    plt.xlabel("BLUE vol")
    plt.grid()

    plt.subplot(222)
    plt.plot(datetime, close, color='black')
    plt.xlabel("BLACK close")
    plt.grid()

    fig.set_size_inches(15, 10)

    path = "C:/image/"+c.DATE.replace("-","")+"/"
    if not os.path.exists(path):
        os.mkdir(path)
    plt.savefig(path+title[:6]+".jpg")
    #plt.show()
    plt.close()

'''
def tor_vr(start,end,code,para):
    sql = "select datetime,close,tor,vr,vol from t_daydata where datetime between '"+start+"' and '"+end+"' and code = '"+code+"'"
    title = code+" ["+start+","+end+" ] "+para
    datetime = []
    close = []
    tor = []
    vr = []
    vol = []
    for row in hq._excutesql(sql):
        datetime.append(row[0])
        close.append(row[1])
        tor.append(row[2])
        vr.append(row[3])
        vol.append(row[4]/100000)
    # 创建子图
    fig, ax = plt.subplots()
    #fig.subplots_adjust(bottom=0.2)
    plt.figure(1,figsize=(150, 130))
    #plt.subplot(212)
    # 设置X轴刻度为日期时间
    ax.xaxis_date()
    plt.title(title)
    plt.xticks()#pd.date_range(start,end))
    plt.yticks()
    plt.xlabel("BLACK close,YELLOW tor,GREEN vr,BLUE vol")
    plt.ylabel("")
    plt.plot(datetime,close,color = 'black')
    plt.plot(datetime, tor,color = 'yellow')
    plt.plot(datetime, vr,color = 'red')
    plt.xlabel("YELLOW tor,RED vr")
    plt.plot(datetime, vol, color='blue')
    plt.grid()
    fig.set_size_inches(15, 10)
    plt.savefig("C:/image/"+title[:6]+".jpg")
    plt.show()
    plt.close()
'''

def check_record(start,end):
    s = datetime.datetime.now()
    #sql = "select code,type from t_record where type <> 'get_aoi' and date ='"+c.DATE+"'"
    #sql = "select code,pe from t_stockbasics order by code "
    sql = "select code,'' from t_daydata where datetime = '2018-07-04' and vr > 1 order by code "
    for row in hq._excutesql(sql):
        tor_vr(start,end,row[0],row[1])
    f = datetime.datetime.now()
    print("running :"+str(f-s))