# -*- coding:utf-8 -*-
import QueryData
import configparser
import hashlib
import time
import ast
import writelogging
import datetime
import sys
start_date = (datetime.datetime.now()-datetime.timedelta(days=30)).strftime("%Y-%m-%d")
end_date = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")

limit = 1000
extract_sql =\
    "SELECT v_invitation.student_id,v_invitation.sales_demo,v_invitation.s_d_time update_time,v_invitation.is_show,v_invitation.creator_id emp_id,user_name emp_name,department_id dept_id, v_student.create_time,v_invitation.visit_campus,v_invitation.validity,v_student.source_campus campus_name\
     FROM `v_invitation` left join v_user on v_invitation.creator_id = v_user.user_id left join v_student on v_invitation.student_id = v_student.student_id\
     where (date(v_invitation.s_d_time) between  '%s' and '%s')\
     and v_user.department_id in(74,75,76) \
     and (v_invitation.sales_demo like '%%第一次约访%%')\
     and v_invitation.student_id in(select student_id from v_student where vreg_type not in('带到访','转介绍','公交站牌广告','W-In','C-In电话','个人渠道','陌拜数据') and is_deleted = 0 and owner_role_id > 0)\
     limit %d,"+str(limit)
delete_before_insert = "delete from bi_tmk_info where date(update_time) between '%s' and '%s' "%(start_date,end_date)
extract_load = "insert into bi_tmk_info (emp_id,emp_name,dept_id,group_name,student_id,campus_name,visit_campus,validity,create_time,update_time,is_show)\
                            values(%(emp_id)s,%(emp_name)s,%(dept_id)s,%(group_name)s,%(student_id)s,%(campus_name)s,%(visit_campus)s,%(validity)s,%(create_time)s,%(update_time)s,%(is_show)s)\
                         "
class TransformTmk():
    tmk_result = {}
    def __init__(self):
        self.cp = configparser.ConfigParser()
        self.cp.read("config.cfg",encoding="utf-8")
        self.status_dict = ast.literal_eval(self.cp.get("mapping","status_dict"))
        self.emp_status = ast.literal_eval(self.cp.get("mapping","emp_status"))
    def extract(self):
        self.queryData = QueryData.QueryData()
        self.queryData.connect()
        conn_load =  QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        conn_load.connect()
        conn_load.update(delete_before_insert)
        conn_load.commit()
        times = 0
        while True:
            result = self.queryData.query(extract_sql%(start_date,end_date,times*limit))
            total = len(result)
            print ("Tmk抽取数量  %s"%total)
            for entry in result:
                # 处理需要转换的数据
                if entry["dept_id"] == 74:
                    entry["group_name"] ="成人组"
                elif entry["dept_id"] == 75:
                    entry["group_name"] ="少儿组"
                elif entry["dept_id"] == 76:
                    entry["group_name"] ="跟进组"
                else:
                    entry["group_name"] ="null"
                for field in entry:
                    value = entry[field]
                    if (type(value)==str and value!="null"):
                        entry[field] = "'"+value+"'"
                conn_load.update(extract_load%entry)
            times+=1
            conn_load.commit()
            time.sleep(1)
            if ( total < limit ):
                print ("完成TMK主题抽取......")
                self.queryData.close()
                conn_load.close()
                break

    def dataDistinct(self,result):
        unique = {}
        for entry in result:
            unique_field = "%s%s"%(entry["student_id"],entry["emp_id"])
            if (not unique.get(unique_field)):
                unique[unique_field] = [entry]
            elif( str(entry["update_time"]) >str(unique[unique_field][0]["update_time"]) ):
                unique[unique_field] = [entry]
        return unique
    def transform(self):
        sql ="select * from bi_tmk_info\
            where date(update_time) = '%s'"
        unique = {}
        # 用来查询有没有更早的新单
        self.queryData = QueryData.QueryData()
        self.queryData.connect()
        conn_query =  QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        conn_query.connect()
        # subSql = "select concat(emp_id,date(create_time)) from bi_tmk_info where date(update_time) = date_sub(curdate(),interval 1 day)"
        # result4update  = conn_query.query(subSql)
        # result4update = [value["concat(emp_id,date(create_time))"] for value in result4update]
        # # 避免因为一个结果生成类似("",)的元组,在sql中会产生错误
        # result4update.extend(["查询无结果"+str(datetime.datetime.now())]*2)
        # result4update = tuple(result4update)
        global start_date
        while start_date<=end_date:
            result = conn_query.query(sql%start_date)
            unique.update(self.dataDistinct(result))
            start_date=datetime.datetime.strptime(start_date,"%Y-%m-%d")+datetime.timedelta(days=1)
            start_date=start_date.strftime("%Y-%m-%d")
            time.sleep(1)

        # 遍历所有记录,去最新数据，当天多个电话去重
        for index,_entry in enumerate(unique.values()):
            _entry=_entry[0]
            source_data =("%s%s%s"%(_entry["emp_id"],_entry["campus_name"],_entry["update_time"][0:10]))
            source_hash = hashlib.md5(source_data.encode("utf-8")).hexdigest()
            if source_hash not in self.tmk_result:
                self.tmk_result[source_hash] = [{\
                    "emp_id":_entry["emp_id"],\
                    "emp_name":_entry["emp_name"],\
                    "calculate_date":_entry["update_time"][0:10],\
                    "campus_name":_entry["campus_name"],\
                    "group_name":_entry["group_name"],\
                    "order_total":0,\
                    "order_new":0,\
                    "order_old":0,\
                    "invite_success_total":0,\
                    "contact_total":0,\
                    "order_invalid_num":0,\
                    "order_valid_num":0,\
                    "order_unknown_num":0\
                }]
            load_entry = self.tmk_result[source_hash][0]
            load_entry["order_total"] += 1
            if _entry["validity"] in["邀约成功"] :
                load_entry["invite_success_total"] += 1
            if _entry["validity"] in["再联系"] :
                load_entry["contact_total"] += 1
            if _entry["validity"] in ["无效"]:
                load_entry["order_invalid_num"] += 1
            if _entry["validity"] in ["未知"]:
                load_entry["order_unknown_num"] += 1
            if _entry["validity"] in ["有效"]:
                load_entry["order_valid_num"] += 1
            result = self.queryData.query("select count(*) count from v_invitation where student_id=%s and date(s_d_time)<'%s'"%(_entry["student_id"],_entry["update_time"][0:10]))
            if ( not result[0]["count"]):
                load_entry["order_new"] += 1
            load_entry["order_old"] = load_entry["order_total"]-load_entry["order_new"]
        self.queryData.close()
        conn_query.close()
        print ("完成tmk主题转换")
    def load(self):
        load_insert = "insert into bi_tmk_theme(\
        emp_id,emp_name,group_name,order_total,order_new,order_old,campus_name,invite_success_total,\
        contact_total,order_invalid_num,calculate_date,order_unknown_num,order_valid_num)\
        values(%(emp_id)s,%(emp_name)s,%(group_name)s,%(order_total)s,%(order_new)s,%(order_old)s,\
        %(campus_name)s,%(invite_success_total)s,%(contact_total)s,%(order_invalid_num)s,%(calculate_date)s,\
        %(order_unknown_num)s,%(order_valid_num)s)\
        on duplicate key update emp_name=values(emp_name),\
                                group_name=values(group_name),\
                                order_total=values(order_total),\
                                order_new=values(order_new),\
                                order_old=values(order_old),\
                                campus_name=values(campus_name),\
                                invite_success_total=values(invite_success_total),\
                                contact_total=values(contact_total),\
                                order_invalid_num=values(order_invalid_num),\
                                order_unknown_num=values(order_unknown_num),\
                                order_valid_num=values(order_valid_num)"
        conn =  QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        conn.connect()
        # 目标表中，如果员工号，校区，时间，有一项不同，就需要insert,完全相同则是update
        for hash_key in self.tmk_result:
            per_theme_data = self.tmk_result[hash_key][0]
            for field in per_theme_data:
                if type(per_theme_data[field]) == str and per_theme_data[field] !="null":
                    per_theme_data[field]="'"+per_theme_data[field]+"'"
            conn.update(load_insert%per_theme_data)
        conn.commit()
        print ("完成TMK主题存储......")
        conn.close()
        writelogging.logger.info("TMK主题数据装载结束,本次%s条记录"%len(self.tmk_result))
        self.tmk_result={}
