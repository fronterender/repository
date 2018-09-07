# -*- encoding:utf-8 -*-
# import pymysql
import configparser
import time

cp = configparser.ConfigParser()
cp.read("config.cfg",encoding="utf-8")

def sqlHandler(**info):
    queryData = info["queryData"]
    sql = info["sql"]
    sqlType = info["sqlType"]
    if "piece" not in info.keys():
        # 没有指定一个批次处理数量，则读取默认值
        info["piece"]=cp.get("DML","pagesize")
    piece = info["piece"]
    if queryData.conn==None:
        queryData.connect()
    times = 0
    resultSet = []
    while True:
        if sqlType == "query":
            result_info = queryData.query(sql+" limit %s,%s"%(times*piece,piece))
            total = len(result_info)
            for item in result_info:
                resultSet.append(item)
            times+=1
            print ("查询次数为%s"%times)
            time.sleep(2)
            if total<piece:
                return (resultSet)
    
