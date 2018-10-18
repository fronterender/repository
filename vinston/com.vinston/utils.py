# -*- coding:utf-8 -*-
import pymysql
import configparser
import time
import writelogging
cp = configparser.ConfigParser()
cp.read("config.cfg",encoding="utf-8")

def sqlHandler(**info):
    queryData = info["queryData"]
    sql = info["sql"]
    sqlType = info["sqlType"]
    if info.get("piece") == None:
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
            print ("本次抽取%s个"%total)
            resultSet.extend(result_info)
            times+=1
            time.sleep(1)
            if total<piece:
                return (resultSet)
def distinct(container,*fields):
    _str = list(fields)
    _str = ["%s"%i for i in _str]
    a,b=0,0
    if (not container or not isinstance(container,list) ):
        return ("去重数据必须为列表且不为空")
    for item in container:
        if not isinstance(item,(str,int,dict)):
            return "同时只能包含数字加字符串或字典这两种中的一种"
        if (isinstance(item,(str,int))):
            a+=1
        if (isinstance(item,dict)):
            b+=1
    if (a>0 and b>0 ):
        return "同时只能包含数字加字符串或字典这两种中的一种"
    if (a>0 and b==0):
        container=list(set(container))
        return container
    result = []
    if(a==0 and b>0):
        if not fields:
            result = []
            for every in container:
                if every not in result:
                    result.append(every)
            return result
        se = set()
        for every in container:
            source_data = ""
            for field in _str:
                if every.get(field)!=None:
                    source_data+=str(every.get(field))
            if source_data not in se:
                result.append(every)
                se.update([source_data])
        return result
