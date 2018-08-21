import pymysql
import vinston_dao as dao
import datetime


class QueryData(dao.Dao):
    db_basic = {}
    conn = None
    def __init__(self,host="192.168.0.200",user="vinston",pwd="vinston@1",dbname="vinston"):
        self.db_basic["host"] = host
        self.db_basic["user"] = user
        self.db_basic["pwd"] = pwd
        self.db_basic["dbName"] = dbname
        dao.Dao.__init__(self)
    def connect(self):
        try:
            self.conn = pymysql.connect(\
            self.db_basic["host"],\
            self.db_basic["user"],\
            self.db_basic["pwd"],\
            self.db_basic["dbName"]\
            )
        except Exception as e:
            raise
        return self.conn
    def close(self):
        self.conn.close()
    def query(self,sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        # 查询结果集，不包含列名称
        resultSet = cursor.fetchall()
        # 列名称
        column_names = [ column_touple[0] for column_touple in cursor.description]
        resultList = []
        for entry in resultSet:
            item = {}
            for index,columen_name in enumerate(column_names):
                _entry = entry[index]
                if (type(_entry) == datetime.datetime):
                    _entry = _entry.strftime("%Y-%m-%d %H:%M:%S")
                item[columen_name] = _entry
            resultList.append(item)
        return resultList
    def updata(self,sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        self.conn.commit()
