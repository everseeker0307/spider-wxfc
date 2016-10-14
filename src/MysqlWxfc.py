#!/usr/local/bin/python3
# coding:utf-8

import pymysql
import time
from datetime import date
from datetime import timedelta
from log import logger

class MysqlWxfc:
    def openDB(self, ip='localhost', username='root', password='333', db='wxfc'):
        db = pymysql.connect(ip, username, password, db, charset='utf8')
        return db

    def getHouseNum(self):
        '''获得数据库中的楼盘总量'''
        db = self.openDB()
        cursor = db.cursor()
        cursor.execute('select count(*) from houseinfo')
        data = cursor.fetchone()
        db.close()
        return data[0]

    def getStock(self, date):
        '''获得截止到date那天的库存总量'''
        db = self.openDB()
        cursor = db.cursor()
        cursor.execute('select forsale from dailyinfo where date = \'{}\''.format(date))
        data = cursor.fetchall()
        db.close()
        stock = 0
        for eachStock in data:
            stock = stock + eachStock[0]
        return stock

    def getDailyVOL(self, dt):
        '''获得date那天的成交量: [id, name, vol]'''
        the_day = dt
        if isinstance(dt, str):
            t_s_year, t_s_mon, t_s_day = dt.split('-')
            the_day = date(int(t_s_year), int(t_s_mon), int(t_s_day))
        previous_day = the_day - timedelta(days = 1)
        db = self.openDB()
        cursor = db.cursor()
        # 查询每个楼盘前一天和当天库存，格式为(楼盘id，'楼盘名称'，'前一天库存, 当天库存')
        sql = 'select d.house_id, h.name, group_concat(d.forsale order by d.date asc) from dailyinfo d join houseinfo h on d.date in (\'{}\', \'{}\') and h.id=d.house_id group by d.house_id order by d.house_id desc'.format(previous_day, the_day)
        cursor.execute(sql)
        data = cursor.fetchall()
        db.close()
        dailyVOL = []
        for each in data:
            stock = each[2].split(',')
            if len(stock) == 2:
                vol = int(stock[0]) - int(stock[1])
                if vol != 0:
                    dailyVOL.append((each[0], each[1], vol))
        return dailyVOL

    def saveSingleToHouseTable(self, record):
        '''单条记录存入数据库'''
        db = self.openDB()
        cursor = db.cursor()
        sql = "insert into houseinfo(id, name, total, licence, createdate) values({0}, '{1}', {2}, '{3}', '{4}')".format(record[0], record[1], record[2], record[3], record[4])
        while True:
            try:
                cursor.execute(sql)
                db.commit()
                db.close()
                return
            except Exception as e:
                logger.warning(e)
                db.rollback()
                time.sleep(2)

    def saveToDailySecondTable(self, record):
        '''每日二手房成交量存入数据表secdailyinfo'''
        db = self.openDB()
        cursor = db.cursor()
        sql = "insert into secdailyinfo(date, vol, area) values('{0}', {1}, {2}) on duplicate key update vol={1}, area={2}".format(record[0], record[1], record[2], record[1], record[2])
        cursor.execute(sql)
        while True:
            try:
                db.commit()
                db.close()
                return
            except Exception as e:
                logger.warning(e)
                db.rollback()
                time.sleep(2)

    def saveManyToDailyTable(self, records):
        '''批量记录存入数据库'''
        db = self.openDB()
        cursor = db.cursor()
        for record in records:
            # 如果不存在则插入，如果存在则更新
            # alter table dailyinfo add unique index (house_id, date)
            sql = "insert into dailyinfo(id, house_id, forsale, date) values('{0}', {1}, {2}, '{3}') on duplicate key update forsale={4}".format(record[0], record[1], record[2], record[3], record[2])
            cursor.execute(sql)
        while True:
            try:
                db.commit()
                db.close()
                return
            except Exception as e:
                logger.warning(e)
                db.rollback()
                time.sleep(2)

    def isExistHouseInfoById(self, id):
        '''根据id查询楼盘是否存在'''
        db = self.openDB()
        cursor = db.cursor()
        cursor.execute('select * from houseinfo where id=' + id)
        data = cursor.fetchone()
        db.close()
        if (data != None):
            return True
        return False
