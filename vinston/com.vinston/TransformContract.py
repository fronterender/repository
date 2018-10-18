# -*- coding:utf-8 -*-
import QueryData
import configparser
import utils
import datetime
import time
import writelogging
import sys

start_date=(datetime.datetime.now()-datetime.timedelta(days=10)).strftime("%Y-%m-%d")
end_date=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# default_calc_date="2018-09-21"
extract_sql="SELECT contract.contract_id,contract.creator_role_id emp_id,u.name emp_en_name,u.user_name emp_cn_name,contract.student_id,contract.create_time,contract.contract_amount,contract.sex,contract.campus_id,t.name campus_name,r.introduce_id,introduce.type\
             FROM `v_contract` contract LEFT JOIN v_r_contract_course r on contract.contract_id = r.contract_id\
                                        LEFT JOIN v_course_introduction introduce on introduce.introduce_id =  r.introduce_id\
									    LEFT JOIN v_teaching_campus t on contract.campus_id = t.campus_id\
                                        LEFT JOIN v_user u on contract.creator_role_id = u.user_id\
            where FROM_UNIXTIME(contract.create_time,'%%Y-%%m-%%d') between '%s' and '%s'"%(start_date,end_date)
extract_load = "insert into bi_contract_info(contract_id,emp_id,emp_en_name,emp_cn_name,student_id,introduce_id,campus_id,campus_name,sex,create_date,type,contract_amount)\
                                     values(%(contract_id)s,%(emp_id)s,%(emp_en_name)s,%(emp_cn_name)s,%(student_id)s,%(introduce_id)s,%(campus_id)s,%(campus_name)s,%(sex)s,%(create_date)s,%(type)s,%(contract_amount)s)\
                                     on duplicate key update emp_id=values(emp_id),emp_en_name=values(emp_en_name),\
                                     emp_cn_name=values(emp_cn_name),campus_id=values(campus_id),campus_name=values(campus_name),\
                                     sex=values(sex),type=values(type),contract_amount=values(contract_amount)"
delete_before_insert = "delete from bi_contract_info where create_date between '%s' and '%s' "%(start_date,end_date)
class TransformContract():
    def __init__(self):
        self.cp = configparser.ConfigParser()
        self.cp.read("config.cfg",encoding="utf-8")
    def extract(self):
        self.queryData = QueryData.QueryData()
        self.queryData.connect()
        result = utils.sqlHandler(queryData=self.queryData,sqlType="query",sql=extract_sql,piece = 10000)
        self.queryData.close()
        conn_load =  QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        conn_load.connect()
        conn_load.update(delete_before_insert)
        conn_load.commit()
        time.sleep(2)
        for item in result:
            for key in item:
                if type(item[key]) == str and item[key] != "null":
                    item[key] = "'"+item[key]+"'"
            item["create_date"] = time.strftime("%Y-%m-%d",time.localtime(item["create_time"]))
            item["create_date"] = "'"+item["create_date"]+"'"
            conn_load.update(extract_load%item)
        print ("完成合同主题抽取、转换、存储......")
        conn_load.commit()
        conn_load.close()
    def transform(self):
        pass
    def load(self):
        pass
