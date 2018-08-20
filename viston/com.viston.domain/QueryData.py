import pymysql
import vinston_dao as dao
import datetime

class QueryData(dao.Dao):
    def __init__(self):
        dao.Dao.__init__(self)
    def connect(self,dbtype,ip,usr,pwd,database):
        if (type(dbtype)!=str or dbtype.find("mysql")>=0):
            try:
                conn = pymysql.connect(ip,usr,pwd,database)
            except:
                print ("数据库连接失败")
                # 部署之前，需对pass代码处的遗留问题处理
                pass
            return conn
    def close(self,conn):
        conn.close()
    def query(self,conn,sql):
        cursor = conn.cursor()
        cursor.execute(sql)
        # 查询结果集，不包含列名称
        resultSet = cursor.fetchall()
        # 列名称
        column_names = [ column_touple[0] for column_touple in cursor.description]
        resultList = []
        return (resultSet[0][12])
        # for entry in resultSet:
        #     item = {}
        #     for index,columen_name in enumerate(column_names):
        #         item[columen_name] = entry[index]

        # 部署之前，需对pass代码处的遗留问题处理
        pass
    def updata(self,conn,sql):
        pass
queryData = QueryData()
conn = queryData.connect("mysql","192.168.0.200","vinston","vinston@1","vinston")
date = queryData.query(conn,"select * from v_student_2")
cursor = conn.cursor()
dt = date
# print (date.strftime("%Y-%m-%d %H:%M:%S"))
sql = "INSERT into vinston.clock(`日期`) values(str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" %(dt.strftime("%Y-%m-%d %H:%M:%S"))
print (sql)
cursor.execute(sql)
# print (type())
queryData.close(conn)
