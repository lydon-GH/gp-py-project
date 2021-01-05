import baostock as bs
import pandas as pd
import pymysql
import datetime
#### 获取沪深A股历史K线数据 ####
# 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
# 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
# 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
def getShareData(shareCode,startDate,endDate):
    rs = bs.query_history_k_data_plus("sz.000651",
                                      "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
                                      start_date=startDate, end_date=endDate,
                                      frequency="d", adjustflag="3")
    print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    return data_list

def saveData(dataList):
    for data in dataList:
        dateStr=data.__getitem__(0)
        code=data.__getitem__(1)
        open=data.__getitem__(2)
        high=data.__getitem__(3)
        low=data.__getitem__(4)
        close=data.__getitem__(5)
        preclose=data.__getitem__(6)
        volume=data.__getitem__(7)
        amount=data.__getitem__(8)
        adjustflag=data.__getitem__(9)
        turn=data.__getitem__(10)
        tradestatus=data.__getitem__(11)
        pctChg=data.__getitem__(12)
        isST=data.__getitem__(13)
        sql = "insert into tb_share_day values ('"+code+"','"+dateStr+"','"+open+"','"+high+"','"+low+"','"+close+"','"+preclose+"','"+volume+"','"+amount+"','"+adjustflag+"','"+turn+"','"+tradestatus+"','"+pctChg+"','"+isST+"',"+"null,'"+"','"+"','"+"')"
        print(sql)
        cursor.execute(sql)
        db.commit()

def fillAverageLine(daycount):
    sql = "select * from tb_share_day order by date asc "
    cursor.execute(sql)
    shareDayData = cursor.fetchall()
    shareDayList=list(shareDayData)
    for day in shareDayList:
        closePrice=day.__getitem__(5)
        curDate=day.__getitem__(1)
        curDateStr = curDate.strftime( "%Y-%m-%d %H:%M:%S")
        cursor.execute("select * from tb_share_day sd where sd.DATE<='"+curDateStr+"' ORDER BY DATE DESC  LIMIT 0,"+str(daycount))
        fineDays=cursor.fetchall()
        sum=0.0;
        count=0;
        for d in fineDays:
            sum=sum+float(d.__getitem__(5))
            count=count+1
        avg=round(sum/count,2)
        updateClumn=0;
        if(daycount==5):
            updateClumn="fifth_line"
        else:
            updateClumn="tenth_line"

        cursor.execute("update tb_share_day set "+updateClumn+"='"+str(avg)+"' where id ="+str(day.__getitem__(14)))
        db.commit()

def initData():
    cursor.execute("delete from tb_share_day")
    sql = "select * from tb_share_list"
    cursor.execute(sql)
    shareData = cursor.fetchall()
    sharelist=list(shareData)
    print(sharelist)
    startDate='2018-01-01'
    endDate='2020-12-31'
    for data in sharelist:
        shareCode=data.__getitem__(0)
        dataList=getShareData(shareCode,startDate,endDate)
        saveData(dataList)
#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)
db = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='gp-resource', charset='utf8')
cursor = db.cursor()
initData()
fillAverageLine(5)
fillAverageLine(10)





# result = pd.DataFrame(data_list, columns=rs.fields)
# #### 结果集输出到csv文件 ####
# result.to_csv("D:\\history_A_stock_k_data.csv", index=False)
# print(result)

#### 登出系统 ####
bs.logout()
