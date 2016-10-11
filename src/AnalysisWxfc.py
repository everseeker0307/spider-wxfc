#!/usr/local/bin/python3
# coding:utf-8

from MysqlWxfc import MysqlWxfc
from datetime import date
from datetime import timedelta

class AnalysisWxfc:

    def __init__(self):
        self.wxfc = MysqlWxfc()

    def getStock(self, date):
        '''获得截止到date那天的库存总量'''
        return self.wxfc.getStock(date)

    def getPeriodStock(self, startDate, endDate):
        '''获得startDate到endDate之间的每日库存量, startDate和endDate为str'''
        result = []
        day = startDate
        end_day = endDate
        if isinstance(startDate, str):
            start_s_year, start_s_mon, start_s_day = startDate.split('-')
            day = date(int(start_s_year), int(start_s_mon), int(start_s_day))
        if isinstance(endDate, str):
            end_s_year, end_s_mon, end_s_day = endDate.split('-')
            end_day = date(int(end_s_year), int(end_s_mon), int(end_s_day))
        while day <= end_day:
            dailyStock = self.getStock(day)
            if dailyStock != 0:
                result.append((day, dailyStock))
            day = day + timedelta(days = 1)
        return result

    def getDailyVOL(self, date):
        dailyVOL = self.wxfc.getDailyVOL(date)
        # 排序
        for each in sorted(dailyVOL, key = lambda record: record[2], reverse = True):
            print(each)

if __name__ == '__main__':
    ana = AnalysisWxfc()
    # result = ana.getPeriodStock('2016-09-28', date.today())
    # for daily in result:
    #     print('{}    {}'.format(daily[0], daily[1]))
    ana.getDailyVOL('2016-10-10')
