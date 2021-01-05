import baostock as bs
import pandas as pd
import pymysql
import datetime

def anaylize(shareCode,beginDate):
    print("开始分析"+shareCode)
    sql = "select * from tb_share_day sd where sd.share_code='"+shareCode+"'and  sd.DATE>='"+beginDate+"' ORDER BY DATE ASC "
    cursor.execute(sql)
    shareDayData = cursor.fetchall()
    shareDayList=list(shareDayData)
    upContinuity=0;
    downContinuity=0;
    continuityStartDate=""
    continuityStartPrice=0
    lastDate=""
    lastDatePrice=float(shareDayList.__getitem__(0).__getitem__(5))
    allmoney=20000
    waitForBuy=False
    waitForSole=False
    for inx, day in enumerate(shareDayList):
        closePrice=float(day.__getitem__(5))
        fifthPrice=float(day.__getitem__(15))
        tenthPrice=float(day.__getitem__(16))
        curDate=day.__getitem__(1)
        curDateStr = curDate.strftime( "%Y-%m-%d %H:%M:%S")

        if(closePrice<lastDatePrice):
            upContinuity=0
            downContinuity=downContinuity+1

            cute=(lastDatePrice-closePrice)/lastDatePrice
            print(curDateStr+"今日下跌"+str(round(cute*100,2))+"%")
            if(downContinuity>2):
                print("自从"+continuityStartDate+"以来，已经连续下跌"+str(downContinuity)+"天,跌了"+str(round((closePrice-continuityStartPrice)/continuityStartPrice*100,2))+"%，请注意")
                waitForBuy=True

            if(closePrice<fifthPrice):
                print("跌破5日均线"+str(round((fifthPrice-closePrice)/fifthPrice*100,2))+"%，请注意")
                if(waitForSole):
                    allmoney=allmoney+closePrice*200
                    print("====因为跌破五日均线后选择卖出，得到"+str(allmoney)+"等待下一次时机")
                    waitForSole=False

            if(closePrice<tenthPrice):
                print("跌破10日均线，请注意")
        else:
            upContinuity=upContinuity+1
            downContinuity=0;
            cute=(closePrice-lastDatePrice)/lastDatePrice
            print(curDateStr+"今日上涨"+str(round(cute*100,2))+"%")
            if(closePrice>fifthPrice):
                print("高升破5日均线"+str(round((closePrice-fifthPrice)/fifthPrice*100,2))+"%，请注意上涨时期")
                if(waitForBuy):
                    allmoney=allmoney-closePrice*200
                    waitForBuy=False
                    waitForSole=True
                    print("====因为大跌后大涨突破5日均线后选择买入，还剩"+str(allmoney))
                if(closePrice>tenthPrice):
                    print("高升破10日均线"+str(round((closePrice-tenthPrice)/tenthPrice*100,2))+"，请注意上涨时期")


        if(upContinuity>2):
                print("continuityStartPrice="+str(continuityStartPrice))
                print("自从"+continuityStartDate+"以来，已经连续上涨"+str(upContinuity)+"天，上涨"+str(round((closePrice-continuityStartPrice)/continuityStartPrice*100,2))+"%，请注意")
        if(upContinuity<2 and downContinuity<2):
            continuityStartDate=curDateStr
            continuityStartPrice=lastDatePrice
        lastDate=curDateStr
        lastDatePrice=closePrice
    print("最后的总资本为"+str(allmoney))
def anaylizeAll(beginDate):
    sql = "select * from tb_share_list"
    cursor.execute(sql)
    shareData = cursor.fetchall()
    sharelist=list(shareData)
    for data in sharelist:
        shareCode=data.__getitem__(0)
        anaylize(shareCode,beginDate)
#### 登陆系统 ####
print("开始分析")
db = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='gp-resource', charset='utf8')
cursor = db.cursor()
startDate='2018-01-01'
anaylizeAll(startDate)




# result = pd.DataFrame(data_list, columns=rs.fields)
# #### 结果集输出到csv文件 ####
# result.to_csv("D:\\history_A_stock_k_data.csv", index=False)
# print(result)

#### 登出系统 ####
bs.logout()
