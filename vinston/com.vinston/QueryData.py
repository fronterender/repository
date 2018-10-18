# -*- encoding:utf-8 -*-
import pymysql
import vinston_dao as dao
import datetime
import configparser
import sys
import writelogging


cp = configparser.ConfigParser()
cp.read("config.cfg",encoding="utf-8")
class QueryData(dao.Dao):
    db_basic = {}
    conn = None
    #程序读取配置文件,默认为None
    def __init__(self,host=cp.get("mysql","host"),\
                      user=cp.get("mysql","user"),\
                      pwd=cp.get("mysql","pwd"),\
                      dbname=cp.get("mysql","dbname")\
                ):
        self.db_basic["host"] = host
        self.db_basic["user"] = user
        self.db_basic["pwd"] = pwd
        self.db_basic["dbname"] = dbname
        dao.Dao.__init__(self)
    def connect(self):
        try:
            self.conn = pymysql.connect(\
            self.db_basic["host"],\
            self.db_basic["user"],\
            self.db_basic["pwd"],\
            self.db_basic["dbname"]\
            )
        except Exception as e:
            writelogging.logger.error("数据库连接失败")
    def close(self):
        try:
            self.conn.close()
        except Exception as e:
            writelogging.logger.error("数据库关闭失败")

    def query(self,sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            # 查询结果集，不包含列名称
            resultSet = cursor.fetchall()
            # 列名称
            column_names = [ column_touple[0] for column_touple in cursor.description]
        except Exception as e:
            print (e)
            writelogging.logger.error("数据库查询失败")
            sys.exit()
        resultList = []
        for entry in resultSet:
            item = {}
            for index,columen_name in enumerate(column_names):
                _entry = entry[index]
                if (type(_entry) == datetime.datetime):
                    _entry = _entry.strftime("%Y-%m-%d %H:%M:%S")
                if (type(_entry) == datetime.date):
                    _entry = _entry.strftime("%Y-%m-%d")
                if ( _entry == None):
                    _entry = "null"
                item[columen_name] = _entry
            resultList.append(item)
        return resultList
    def update(self,sql):
        # 性能考虑,commit提交不设置在这里，单独提交，可用commit方法
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
        except Exception as e:
            print (e)
            writelogging.logger.error("数据更新失败，检查连接")
            sys.exit()
    def commit(self):
        try:
            self.conn.commit()
        except Exception as e:
            writelogging.logger.error("数据提交失败,检查连接")
            sys.exit()
