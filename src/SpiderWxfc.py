#!/usr/local/bin/python3
# coding:utf-8

import requests
import re
from concurrent.futures import ThreadPoolExecutor
import time
from MysqlWxfc import MysqlWxfc
from datetime import date
import uuid
from log import logger

class SpiderWxfc:

    def getPageContent(self, url='http://www.wxhouse.com:9097/wwzs/getzxlpxx.action'):
        '''获取首页源代码'''
        headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36', 'Connection': 'close'}
        try:
            r = requests.get(url, headers = headers)
            r.close()
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            logger.error(e)
            raise

    def postPageContent(self, url='http://www.wxhouse.com:9097/wwzs/getzxlpxx.action', currentPageNo='1', pageSize='15'):
        '''获取指定页面源代码，默认currentPageNo=1时与getPageContent函数结果一致'''
        headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36', 'Connection': 'close'}
        data = {'page.currentPageNo': currentPageNo, 'page.pageSize': pageSize}
        while (True):
            try:
                r = requests.post(url, headers = headers, data = data)
                r.close()
                if (r.status_code == 200):
                    # print('currentPageNo:', currentPageNo)
                    r.encoding = r.apparent_encoding
                    return r.text
            except Exception as e:
                logger.warning('An exception occurs when open a web page: {}'.format(data))
                logger.warning(e)
                time.sleep(5)

    def getHousesInfo(self, pageContent):
        '''
        分析源代码信息，提取出需要的信息;
        return [(id, name, total, forsale, licence), (), ...]
        '''
        housespat = re.compile(r'<a href="/wwzs/queryLpxxInfo\.action\?tplLpxx.id=(.*?)".*?<span style="color:#6A5ACD;font-size: 16"><b>(.*?)</b>.*?总售：(.*?) 套 可售：(.*?) 套</span> <br/>(.*?)</td>', re.DOTALL)
        houses = housespat.findall(pageContent)
        remap = {
            ord('\t'): '',
            ord('\n'): '',
            ord('\r'): '',
            ord(' '): '',
            ord('（'): '(',
            ord('）'): ')'
        }
        houses = [(each[0], each[1], each[2], each[3], each[4].translate(remap)) for each in houses]
        return houses

    def getPageNum(self, pageContent):
        '''分析源代码，确定总页数'''
        totalPages = re.findall(r'name="page.totalPageCount" value="(.*?)" id="totalPageCount"', pageContent)
        return int(totalPages[0])

    def getHouseNum(self, pageContent):
        '''分析源代码，确定总楼盘量'''
        totalHouses = re.findall(r'value="(.*?)" id="totalSize"', pageContent)
        return int(totalHouses[0])

    def saveToHouseTable(self, pageContent):
        '''获取首页信息，页面总楼盘量和数据库中的比较，如果不一致，说明有新楼盘，将新楼盘信息存入数据库'''
        totalHouseNum = self.getHouseNum(pageContent)
        mysqlWxfc = MysqlWxfc()
        newHouseNum = totalHouseNum - mysqlWxfc.getHouseNum()
        logger.info('today new houseNum: {}'.format(newHouseNum))
        if (newHouseNum > 0):
            totalPageNum = self.getPageNum(pageContent)
            pageIter = 0
            while (pageIter < totalPageNum):
                pageIter = pageIter + 1
                pageContent = self.postPageContent(currentPageNo = str(pageIter))
                houses = self.getHousesInfo(pageContent)
                for house in houses:
                    # 如果该楼盘id不存在，那么将楼盘信息加入数据库
                    if (mysqlWxfc.isExistHouseInfoById(house[0]) == False):
                        record = (int(house[0]), house[1], int(house[2]), house[4], time.strftime('%Y-%m-%d'))
                        mysqlWxfc.saveSingleToHouseTable(record)
                        newHouseNum = newHouseNum - 1
                if (newHouseNum <= 0):
                    break
                time.sleep(1)

    def saveToDailyTable(self, url='http://www.wxhouse.com:9097/wwzs/getzxlpxx.action', currentPageNo='1', pageSize='15'):
        '''将每日楼盘可售数量存入数据库'''
        pageContent = self.postPageContent(currentPageNo = currentPageNo)
        houses = self.getHousesInfo(pageContent)
        records = [(uuid.uuid4().hex, int(each[0]), int(each[3]), time.strftime('%Y-%m-%d')) for each in houses]
        mysqlWxfc = MysqlWxfc()
        mysqlWxfc.saveManyToDailyTable(records)

if __name__ == '__main__':
    start = time.time()
    wxfc = SpiderWxfc()
    # 1、将每日新增楼盘信息存入数据库
    indexPage = wxfc.getPageContent()
    wxfc.saveToHouseTable(indexPage)
    # 2、将每日可售数量存入数据库
    pool = ThreadPoolExecutor(10)
    totalPageNum = wxfc.getPageNum(indexPage)
    pageIter = 0
    while (pageIter < totalPageNum):
        pageIter = pageIter + 1
        pool.submit(wxfc.saveToDailyTable, currentPageNo = str(pageIter))
        time.sleep(1)
    end = time.time()
    # 计算的时间是把所有任务加入线程池中的时间，而不是所有任务已经完成的时间
    logger.info('it costs time {}s'.format(end - start))
