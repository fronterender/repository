# -*- coding:utf-8 -*-
import QueryData
import configparser
import hashlib
import time
import ast
import datetime
import writelogging
import ast
import requests
import sys

start_date=(datetime.datetime.now()-datetime.timedelta(days=10 )).strftime("%Y-%m-%d")
end_date=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
extract_sql = "select student_id,visit_campus,invite_role_id,creator_role_id ,from_unixtime(create_time,'%%Y-%%m-%%d %%H:%%i:%%S') visit_time\
              from v_student_visit\
             where from_unixtime(create_time,'%%Y-%%m-%%d') between '%s' and '%s' \
             "%(start_date,end_date)

class TransformVisit():
    def __init__(self):
        self.visit_result = {}
        self.cp = configparser.ConfigParser()
        self.cp.read("config.cfg",encoding="utf-8")
    def queryDept(self,creator_role_id):
        session = requests.Session()
        res = session.get("http://175.6.37.82:8800/api/common/department/%s"%creator_role_id,timeout=6)
        session.close()
        retult={}
        try:
            retult = ast.literal_eval(res.text)
        except Exception as e:
            print (creator_role_id,"部门解析错误",res.text)
        return retult
    def getTarget(self,resultSet):
        result = []
        studentSet = {}
        for entry in resultSet:
            group = "%s%s"%(entry["student_id"],entry["visit_time"][0:10])
            if group not in studentSet:
                studentSet[group] = [entry]
            if studentSet[group][0]["visit_time"]<entry["visit_time"]:
                studentSet[group] = [entry]
        for item in studentSet.values():
            result.append(item[0])
        return result
    def extract(self):
        delete_before_insert = "delete from bi_visit_info where date(visit_time) between '%s' and '%s' "%(start_date,end_date)
        load_sql = "insert into bi_visit_info(student_id,visit_campus,visit_time,invite_role_id,department_name,v_ftm_id\
        ) values(%(student_id)s,'%(visit_campus)s','%(visit_time)s',%(invite_role_id)s,'%(department_name)s','%(v_ftm_id)s')\
                    on duplicate key update student_id=values(student_id),visit_campus=values(visit_campus),invite_role_id=values(invite_role_id),department_name=values(department_name),v_ftm_id=values(v_ftm_id)"
        self.queryData = QueryData.QueryData()
        self.queryData.connect()
        resultSet = self.queryData.query(extract_sql)
        resultSet = self.getTarget(resultSet)

        conn_load = QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        conn_load.connect()
        conn_load.update(delete_before_insert)
        conn_load.commit()
        for item in resultSet:
            # student_id,visit_campus,invite_role_id,creator_role_id
            department_name = self.queryDept(item["invite_role_id"])
            time.sleep(0.2)
            item["department_name"]=""
            if department_name and department_name.get("data") :
                item["department_name"] = department_name["data"][0]
            if (not item["visit_campus"]):
                item["visit_campus"]=item["source_campus"]
            v_ftm_list = self.queryData.query("select v_ftm from v_student where student_id=%(student_id)s"%item)
            item["v_ftm_id"]=""
            v_ftm = v_ftm_list[0].get("v_ftm")
            if v_ftm:
                item["v_ftm_id"] = v_ftm
            conn_load.update(load_sql%item)
        print ("完成visit主题抽取......")
        conn_load.commit()
        conn_load.close()
        self.queryData.close()
    def transform(self):
        self.queryData = QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        self.queryData.connect()
        sql = "select * from bi_visit_info where date(visit_time)  between '%s' and '%s' "%(start_date,end_date)
        resultSet = self.queryData.query(sql)
        for entry in resultSet:
            source_data = "%s%s"%(entry["visit_campus"],entry["visit_time"][0:10])
            source_hash = hashlib.md5(source_data.encode("utf-8")).hexdigest()
            if self.visit_result.get(source_hash) == None:
                self.visit_result[source_hash]=[{\
                        "visit_date":entry["visit_time"][0:10],\
                        "visit_campus":entry["visit_campus"],\
                        "visit_num":0\
                }]
            load_entry = self.visit_result[source_hash][0]
            load_entry["visit_num"]+=1
        print ("完成visit主题转换......")
    def load(self):
        # delete_before_insert = "delete from bi_visit_theme where visit_date between '%s' and '%s' "%(start_date,end_date)
        load_sql = "insert into bi_visit_theme(visit_campus,visit_num,visit_date) \
                                values(%(visit_campus)s,%(visit_num)s,%(visit_date)s)\
                                on duplicate key update visit_num = values(visit_num)"
        conn =  QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        conn.connect()
        for hash_key in self.visit_result:
            per_theme_data = self.visit_result[hash_key][0]
            for field in per_theme_data:
                if type(per_theme_data[field]) == str and per_theme_data[field] !="null":
                    per_theme_data[field]="'"+per_theme_data[field]+"'"
            conn.update(load_sql%per_theme_data)
        self.visit_result={}
        print ("完成visit主题存储......")
        conn.commit()
        conn.close()
