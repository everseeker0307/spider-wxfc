#!/usr/local/bin/python3
# coding:utf-8

import pymysql
import time

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

    def saveSingleToHouseTable(self, record):
        '''单条记录存入数据库'''
        db = self.openDB()
        cursor = db.cursor()
        sql = "insert into houseinfo(id, name, total, licence, createdate) values({0}, '{1}', {2}, '{3}', '{4}')".format(record[0], record[1], record[2], record[3], record[4])
        while (True):
            try:
                cursor.execute(sql)
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
        while (True):
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
