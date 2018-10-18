# -*- coding:utf-8 -*-
import QueryData
import datetime
import configparser
import writelogging
import ast
import time
import random
import hashlib
import time
import sys
import math
import utils

start_date=(datetime.datetime.now()-datetime.timedelta(days=90)).strftime("%Y-%m-%d")
end_date=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# 每次运行，计算validity_for_once字段的周期范围
static_validity = (datetime.datetime.now()-datetime.timedelta(days=5)).strftime("%Y-%m-%d")
# extract_sql = "select creator_role_id,user_dept,source_campus,u.name,u.user_name,u.status,s.student_id,s.sex,s.create_time,s.update_time,s.validity,s.old_to_new\
#                from v_student as s left join v_user as u on s.creator_role_id = u.user_id\
#                where (date(s.create_time) = date_sub(curdate(),interval 1 day)  or date(s.update_time)=date_sub(curdate(),interval 1 day))\
#                and vreg_type not in ('带到访','转介绍','公交站牌广告','W-In','个人渠道','online-在线咨询','online-网站留言','online-大众点评','online-推广来电','online-58同城','online-离线宝','online-其它','online-手机抓取','online-异业合作','online-朋友圈','online-W-In','online-今日头条','商家岛微信砍价','商家岛微信砍价2')\
#                and is_deleted =0 \
#                and owner_role_id > 0"
# 抽取查询语句
extract_sql = "select creator_role_id,user_dept,source_campus,s.description,u.name,u.user_name,u.status,s.student_id,s.sex,s.create_time,s.update_time,s.validity,s.old_to_new\
               from v_student as s left join v_user as u on s.creator_role_id = u.user_id\
               where (date(s.create_time) between '%s' and '%s')\
               and vreg_type not in ('带到访','转介绍','公交站牌广告','W-In','个人渠道','online-在线咨询','online-网站留言','online-大众点评','online-推广来电','online-58同城','online-离线宝','online-其它','online-手机抓取','online-异业合作','online-朋友圈','online-W-In','online-今日头条','商家岛微信砍价','商家岛微信砍价2')\
               and is_deleted =0 \
               and owner_role_id > 0"%(start_date,end_date)
# 插入数据之前先删除
# delete_before_insert = "delete from bi_sale_info where date(create_time)=date_sub(curdate(),interval 1 day) or date(update_time)=date_sub(curdate(),interval 1 day)"
delete_before_insert = "delete from bi_sale_info where date(create_time) between '%s' and '%s'"%(start_date,end_date)
# 向bi_sale_info中新增抽取到的数据
extract_insert_sql = "insert into bi_sale_info(emp_id,campus_name,\
                                            emp_en_name,emp_cn_name,\
                                            stu_sex,emp_status,emp_state,\
                                            student_id,create_time,\
                                            order_status,state,update_time,old_to_new,description\
                                            )\
                    values( %(creator_role_id)s,'%(source_campus)s','%(name)s',\
                            '%(user_name)s','%(sex)s','%(emp_status)s',%(status)s,\
                            %(student_id)s,'%(create_time)s','%(validity)s',%(state)s,'%(update_time)s','%(old_to_new)s','%(description)s')\
                    "
# 将处理好的主题数据插入表中
load_sql = "insert into bi_sale_theme(emp_id,campus_name,\
                                    emp_name,order_total,\
                                    order_valid_num,order_invalid_num,\
                                    invite_total,contact_total,create_date,order_no_parents\
                                    )\
                    values(%(emp_id)s,'%(campus_name)s','%(emp_name)s',\
                            %(order_total)s,\
                            %(order_valid_num)s,%(order_invalid_num)s,\
                            %(invite_total)s,%(contact_total)s,'%(create_date)s',%(order_no_parents)s)\
                    on duplicate key update order_total=values(order_total),order_valid_num=values(order_valid_num),\
                                            order_invalid_num=values(order_invalid_num),invite_total=values(invite_total),\
                                            emp_name=values(emp_name),contact_total=values(contact_total),order_no_parents=values(order_no_parents)\
                                            "
# 限定每次查询数据量，可以设置局部变量覆盖
limit =5000
class Transform():
    # 配置文件解析器，默认为空
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
            result = utils.sqlHandler(queryData=self.queryData,sqlType="query",sql=extract_sql,piece = 3000)
            self.queryData.close()
        except Exception as e:
            writelogging.logger.error("市场主题数据库连接失败,请检查用户名,密码")
        try:
            # 一下开始插入数据到另外一个数据库
            self.queryData = QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
            self.queryData.connect()
            # 首先清除之前的错误数据
            self.queryData.update(delete_before_insert)
            self.queryData.commit()
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

                if (item["description"] and (item["description"].find("爷")>=0 or item["description"].find("奶")>=0) ):
                    item["description"] = "非双亲陪同"
                else:
                    item["description"]=" "
                # 循环遍历每一项，如果为None,则改为null
                # for key in item.keys():
                #     if (not item[key]):
                #         item[key]="null"

                if (item["validity"]):
                    self.queryData.update(extract_insert_sql%item)
            self.queryData.commit()
            self.queryData.close()
            print ("完成市场主题抽取......")
            writelogging.logger.info("市场信息表抽取完成,一共%s个记录"%len(result))
        except Exception as e:
            writelogging.logger.error("市场主题数据抽取、数据库连接失败")
    def transform(self):
        """
            1、从数据表中读取所有员工信息
            2、循环遍历每个员工，分别计算每个指标，并存储在字典中,如{"A指标":"##","B指标":"##"}
            3、将第2个步骤中的结果存储在数据库中，从而完成转换,插入数据的原则是没有插入，有则修改结果
        """
        self.queryData = QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        self.queryData.connect()
        # subSql = "select concat(emp_id,date(create_time)) from bi_sale_info where date(update_time) = date_sub(curdate(),interval 1 day)"
        # result4update  = self.queryData.query(subSql)
        # result4update = [value["concat(emp_id,date(create_time))"] for value in result4update]
        # result4update = tuple(result4update)
        _sql = "select * from bi_sale_info where date(create_time) between '%s' and '%s'"%(start_date,end_date)
        times = 0
        while True:
            # sql = _sql+" or concat(emp_id,date(create_time)) in"+str(result4update)+" limit %s,%s"%(times*limit,limit)
            result_info = self.queryData.query(_sql+" limit %s, %s"%(times*limit,limit))
            length = len(result_info)
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
                        "order_total":0,\
                        "order_valid_num":0,\
                        "order_invalid_num":0,\
                        "invite_total":0,\
                        "order_no_parents":0,\
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
                if (entry["order_status"] in ["再联系"]):
                    everyGroup["contact_total"] += 1
                if (entry["description"] and entry["description"].find("非双亲")>=0 ):
                    everyGroup["order_no_parents"] += 1
            times+=1
            time.sleep(1)
            if length < limit:
                print ("完成市场主题转换......")
                self.queryData.close()
                break
    def getListOfValidity(self,conn,querySet,date):
        """
            array:查询条件,student_id元组字符串集合
            date:查询条件，需要转换为最终条件
        """
        # 存储每一个学生id和他的回单有效性
        result = {}
        for item in querySet:
            result[item["student_id"]] = "未拨打"
        queryCondition = [item["student_id"] for item in querySet]
        if len(queryCondition) == 0:
            return result
        if len(queryCondition) == 1:
            queryCondition.append(queryCondition[0])
        end = (datetime.datetime.strptime(date,"%Y-%m-%d")+datetime.timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        sql = "select student_id,validity from v_invitation\
               where  invit_id in (\
                                    select max(invit_id) \
                                    from v_invitation \
                                    where student_id in%s and s_d_time<'%s' group by student_id\
                                  )"%(str(tuple(queryCondition)),end)
        _result = conn.query(sql)
        for _item in _result:
            if _item["student_id"] in result:
                result[_item["student_id"]] = _item["validity"]
        return result
    def getData(self,result):
        valid_for_once = 0
        invalid_for_once = 0
        for validity in result.values():
            if validity in ["有效","再联系","邀约成功"]:
                valid_for_once += 1
            elif validity in ["无效"]:
                invalid_for_once += 1
        return {"valid_for_once":valid_for_once,"invalid_for_once":invalid_for_once}
    def calc_static_validity(self,conn,queryData):
        for hash_key in self.emp_result:
            per_theme_data = self.emp_result[hash_key][0]
            if per_theme_data["create_date"] > static_validity:
                querySet = queryData.query("select student_id from bi_sale_info where emp_id=%(emp_id)s and date(create_time)='%(create_date)s'"%per_theme_data)
                result = self.getListOfValidity(conn,querySet,per_theme_data["create_date"])
                data_of_validity = self.getData(result)
                per_theme_data["valid_for_once"] = data_of_validity["valid_for_once"]
                per_theme_data["invalid_for_once"] = data_of_validity["invalid_for_once"]
                queryData.update("update bi_sale_theme set valid_for_once= %(valid_for_once)s,invalid_for_once=%(invalid_for_once)s where emp_id=%(emp_id)s and campus_name = '%(campus_name)s' and create_date='%(create_date)s'"%per_theme_data)
                print ("更新最近四天数据")
        queryData.commit()
        print ("更新四天内静态到访数据完成")
        return per_theme_data
    def load(self):
        # 计算创建日期在三天以内的静态

        """
            整体过程是将处理后的主题类数据上传到数据库服务器
        """
        try:
            self.queryData = QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
            self.queryData.connect()
            conn = QueryData.QueryData()
            conn.connect()
            for hash_key in self.emp_result:
                per_theme_data = self.emp_result[hash_key][0]
                self.queryData.update(load_sql%per_theme_data)

            self.queryData.commit()
            self.calc_static_validity(conn,self.queryData)
            print ("完成市场主题存储......")
            # 数据存储之后，validity_for_once字段
            self.queryData.close()
            conn.close()
            writelogging.logger.info("市场主题数据装载完成,本次%s条记录"%len(self.emp_result))
        except Exception as e:
            writelogging.logger.error("市场主题数据装载失败")
        self.emp_result = {}
