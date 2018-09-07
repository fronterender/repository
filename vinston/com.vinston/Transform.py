# -*- coding:utf-8 -*-
import QueryData
import datetime
import configparser
import ast
import time
import random
import hashlib
import time
import sys
import math
import utils
queryData = QueryData.QueryData("192.168.0.200","vinston","vinston@1","vinston")

resultset = utils.sqlHandler(queryData=queryData,sqlType="query",sql="select * from v_student",piece = 800)

# 向bi_sale_info中插入抽取到的数据
extract_insert_sql = "insert into bi_sale_info(emp_id,campus_name,\
                                            emp_en_name,emp_cn_name,\
                                            stu_sex,emp_status,emp_state,\
                                            student_id,create_time,\
                                            order_status,state,update_time\
                                            )\
                    values( %(creator_role_id)s,'%(source_campus)s','%(name)s',\
                            '%(user_name)s','%(sex)s','%(emp_status)s',%(status)s,\
                            %(student_id)s,'%(create_time)s','%(validity)s',%(state)s,'%(update_time)s')\
                    on duplicate key update emp_en_name=values(emp_en_name),emp_status=values(emp_status),emp_state=values(emp_state),order_status=values(order_status),state=values(state)\
                    "

# 将处理好的主题数据插入表中
load_sql = "insert into bi_sale_theme(emp_id,campus_name,\
                                    emp_name,order_total,\
                                    order_valid_num,order_invalid_num,\
                                    invite_total,contact_total,create_date\
                                    )\
                    values(%(emp_id)s,'%(campus_name)s','%(emp_name)s',\
                            %(order_total)s,\
                            %(order_valid_num)s,%(order_invalid_num)s,\
                            %(invite_total)s,%(contact_total)s,'%(create_date)s')\
                    on duplicate key update order_total=values(order_total),order_valid_num=values(order_valid_num),\
                                            order_invalid_num=values(order_invalid_num),invite_total=values(invite_total),\
                                            contact_total=values(contact_total)\
                    "
# insert  into t(id, num)values(1, 101) on duplicate key update num = values(num)
class Transform():

    cp = None
    #待读取的配置文件中的对象,存储在cfg文件中
    status_dict,emp_status = None,None
    queryData = None
    # 存储主题域数据
    emp_result = {}
    def __init__(self):
        self.cp = configparser.ConfigParser()
        self.cp.read("config.cfg",encoding="utf-8")
        self.status_dict = ast.literal_eval(self.cp.get("mapping","status_dict"))
        self.emp_status = ast.literal_eval(self.cp.get("mapping","emp_status"))
    def extract(self):
        try:
            self.queryData = QueryData.QueryData()
            self.queryData.connect()

            result = self.queryData.query(self.cp.get("DML","sql"))
            self.queryData.close()
        except Exception as e:
            writelogging.logger.info("数据库连接失败,或是sql语句错误")
        try:
            # 一下开始插入数据到另外一个数据库
            self.queryData = QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
            self.queryData.connect()
            for item in result:
                if (not item["status"]):
                    item["status"] ="null"
                    item["emp_status"] = "null"
                elif (item["status"] in self.emp_status):
                    item["emp_status"] = self.emp_status[item["status"]]
                else:
                    item["emp_status"] = "null"
                if (not item["validity"]):
                    item["validity"] = "null"
                    item["state"] = "null"
                elif ( item["validity"] in self.status_dict):
                    item["state"] = self.status_dict[item["validity"]]
                else:
                    item["state"] = "null"
                if item["update_time"] == "null":
                    item["update_time"] = "0000-00-00 00:00:00"
                if item["create_time"] == "null":
                    item["create_time"] = "0000-00-00 00:00:00"
                # 循环遍历每一项，如果为None,则改为null
                for key in item.keys():
                    if (not item[key]):
                        item[key]="null"
                if (item["validity"]):
                    self.queryData.update(extract_insert_sql%item)
            self.queryData.commit()
            self.queryData.close()
        except Exception as e:
            writelogging.logger.error("数据抽取出错,或连接数据库失败，或者返回数据字段处理出错")
            sys.exit(1)
    def transform(self):
        """
            1、从数据表中读取所有员工信息
            2、循环遍历每个员工，分别计算每个指标，并存储在字典中,如{"A指标":"##","B指标":"##"}
            3、将第2个步骤中的结果存储在数据库中，从而完成转换,插入数据的原则是没有插入，有则修改结果
        """
        self.queryData = QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        self.queryData.connect()
        sql = "select * from bi_sale_info limit %d,8000"
        times = 0
        while True:
            result_info = self.queryData.query(sql%(times*8000))
            length = len (result_info)
            for entry in result_info:
                group = "%s%s%s"%(entry["emp_id"],entry["campus_name"],entry["create_time"][0:10])
                hl_md5_obj = hashlib.md5(group.encode("utf-8"))
                # 唯一键，标明订单属于哪一个员工，哪一个校区的订单
                unique = hl_md5_obj.hexdigest()
                if (unique not in self.emp_result.keys()):
                    self.emp_result[unique] =[{\
                        "emp_id":entry["emp_id"],\
                        "emp_name":entry["emp_cn_name"],\
                        "campus_name":entry["campus_name"],\
                        "create_date":entry["create_time"][0:10],\
                        "update_time":entry["update_time"],\
                        "order_total":0,\
                        "order_valid_num":0,\
                        "order_invalid_num":0,\
                        "invite_total":0,\
                        "contact_total":0\

                    }]
                everyGroup = self.emp_result[unique][0]
                everyGroup["order_total"] +=1
                if (entry["order_status"] in ["有效","再联系","邀约成功"]):
                    everyGroup["order_valid_num"] += 1
                if (entry["order_status"] in ["无效"]):
                    everyGroup["order_invalid_num"] += 1
                if (entry["order_status"] in ["邀约成功"]):
                    everyGroup["invite_total"] += 1
                if (entry["order_status"] in ["再联系","未知再联系"]):
                    everyGroup["contact_total"] += 1
            times+=1
            if length < 8000:
                self.queryData.close()
                break
    def load(self):
        """
            整体过程是将处理后的主题类数据上传到数据库服务器
        """
        try:
            self.queryData = QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
            self.queryData.connect()
            for hash_key in self.emp_result:
                per_theme_data = self.emp_result[hash_key][0]
                self.queryData.update(load_sql%per_theme_data)
            self.queryData.commit()
            self.queryData.close()
        except Exception as e:
            raise
        self.emp_result = {}
