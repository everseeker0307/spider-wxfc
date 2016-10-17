#!/usr/local/bin/python3
# coding:utf-8

import sys
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
        '''获得date那天的详细成交信息'''
        dailyVOL = self.wxfc.getDailyVOL(date)
        # 排序打印, 从大到小排序
        dailyVOLNum = 0
        for each in sorted(dailyVOL, key = lambda record: record[2], reverse = True):
            print(each)
            if each[2] > 0:
                dailyVOLNum = dailyVOLNum + each[2]
        print('\ntoday VOL num: {}'.format(dailyVOLNum))

if __name__ == '__main__':
    ana = AnalysisWxfc()
    # 总库存
    print('all stocks: ')
    result = ana.getPeriodStock('2016-09-28', date.today())
    for daily in result:
        print('{}    {}'.format(daily[0], daily[1]))
    # 每日成交详情
    print('\ntoday VOL details: ')
    if len(sys.argv) == 2:
        ana.getDailyVOL(sys.argv[1])
    else:
        ana.getDailyVOL(date.today())
