#!/usr/local/bin/python3
# coding:utf-8

import requests
import re
from datetime import date
from log import logger
from MysqlWxfc import MysqlWxfc

class SecondWxfc:

    def getPageContent(self, url='http://www.wxhouse.com:9098/housestockpub/'):
        '''获取首页源代码'''
        headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36', 'Connection': 'close'}
        while True:
            try:
                r = requests.get(url, headers = headers)
                r.close()
                if r.status_code == 200:
                    r.encoding = r.apparent_encoding
                    return r.text
            except Exception as e:
                logger.error('can\'t open the page: {}'.format(url))
                logger.error(e)
                time.sleep(5)

    def getHousesInfo(self, pageContent):
        '''
        分析源代码信息，提取出需要的信息;
        return (今日成交量, 今日成交面积)
        '''
        housespat = re.compile(r'<td align="center">今日.*?<td align="right"><strong>(.*?)</strong>套.*?<td align="right"><strong>(.*?)</strong>m<sup>2</sup>', re.DOTALL)
        house = housespat.findall(pageContent)[0]
        house = (date.today(), int(house[0]), float(house[1]))
        return house

    def saveToDailySecondTable(self, pageContent):
        record = self.getHousesInfo(pageContent)
        mysqlWxfc = MysqlWxfc()
        mysqlWxfc.saveToDailySecondTable(record)
        logger.info(record)

if __name__ == '__main__':
    sec = SecondWxfc()
    indexPage = sec.getPageContent()
    sec.saveToDailySecondTable(indexPage)
