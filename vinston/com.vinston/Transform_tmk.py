import QueryData
import configparser
import hashlib
import time
import ast

extract_sql =\
    "SELECT v_invitation.student_id,v_invitation.creator_id emp_id,user_name emp_name,department_id dept_id, v_student.create_time,v_invitation.visit_campus,v_invitation.validity,v_student.source_campus campus_name\
    FROM `v_invitation` left join v_user on v_invitation.creator_id = v_user.user_id left join v_student on v_invitation.student_id = v_student.student_id\
    where date(v_student.create_time) = date_sub(curdate(),interval 1 day) and v_user.department_id in(74,75,76) and v_invitation.is_show = 0 and v_invitation.student_id in(select student_id from v_student where vreg_type not in('带到访','转介绍','公交站牌广告','W-In','C-In电话','个人渠道','陌拜数据') and is_deleted = 0 and  owner_role_id > 0)\
    limit %d,700"
extract_load = "insert into bi_tmk_info (emp_id,emp_name,dept_id,group_name,student_id,campus_name,visit_campus,validity,create_time)\
                            values(%(emp_id)s,%(emp_name)s,%(dept_id)s,%(group_name)s,%(student_id)s,%(campus_name)s,%(visit_campus)s,%(validity)s,%(create_time)s)\
                            ON DUPLICATE KEY UPDATE `emp_name` = VALUES(emp_name),`dept_id` = VALUES(dept_id),`group_name` = VALUES(group_name),`visit_campus` = VALUES(visit_campus),`validity` = VALUES(validity)\
                            "
class Transform_tmk():
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

        times = 0
        while True:
            result = self.queryData.query(extract_sql%(times*700))
            total = len(result)
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
            print ("完成第%s次"%times)
            time.sleep(2)
            if ( total < 700 ):
                self.queryData.close()
                conn_load.close()
                return {"msg":"抽取%s个数据"%(times*700+total)}
    def transform(self):
        sql = "select * from bi_tmk_info limit %s,1000"
        conn_query =  QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        conn_query.connect()
        times = 0
        while True:
            result = conn_query.query( sql%(times*1000) )
            times += 1
            length = len(result)
            for entry in result:
                source_data =("%s%s%s"%(entry["emp_id"],entry["campus_name"],entry["create_time"][0:10]))
                source_hash = hashlib.md5(source_data.encode("utf-8")).hexdigest()
                if source_hash not in self.tmk_result:
                    self.tmk_result[source_hash] = [{\
                        "emp_id":entry["emp_id"],\
                        "emp_name":entry["emp_name"],\
                        "create_date":entry["create_time"][0:10],\
                        "campus_name":entry["campus_name"],\
                        "group_name":entry["group_name"],\
                        "order_total":0,\
                        "invite_total":0,\
                        "contact_total":0,\
                        "order_invalid_num":0,\
                        "order_unknown_num":0\
                    }]
                load_entry = self.tmk_result[source_hash][0]
                load_entry["order_total"] += 1
                if entry["validity"] == "邀约成功":
                    load_entry["invite_total"] += 1
                if entry["validity"] == "再联系":
                    load_entry["contact_total"] += 1
                if entry["validity"] == "无效":
                    load_entry["order_invalid_num"] += 1
                if entry["validity"] == "未知":
                    load_entry["order_unknown_num"] += 1
            # 上传数据
            time.sleep(2)
            if length < 1000:
                conn_query.close()
                return {"msg":"抽取%s个数据"%(times*1000+length)}
    def load(self):
        sql = "select emp_id,campus_name,create_date from bi_tmk_theme"
        load_insert = "insert into bi_tmk_theme(emp_id,emp_name,group_name,order_total,campus_name,invite_total,contact_total,order_invalid_num,create_date,order_unknown_num)\
                                               values(%(emp_id)s,%(emp_name)s,%(group_name)s,%(order_total)s,%(campus_name)s,%(invite_total)s,%(contact_total)s,%(order_invalid_num)s,%(create_date)s,%(order_unknown_num)s)"
        load_update ="update bi_tmk_theme set order_total=%(order_total)s invite_total=%(invite_total)s  contact_total=%(contact_total)s order_invalid_num=%(order_invalid_num)s order_unknown_num=%(order_unknown_num)s \
                     where emp_id=%(emp_id)s and campus_name=%(campus_name)s and create_date=%(create_date)s"
        conn =  QueryData.QueryData(host=self.cp.get("mysql","host_2"),user=self.cp.get("mysql","user_2"),pwd=self.cp.get("mysql","pwd_2"),dbname=self.cp.get("mysql","dbname_2"))
        conn.connect()
        check = conn.query(sql)
        fields = []
        # 目标表中，如果员工号，校区，时间，有一项不同，就需要insert,完全相同则是update
        for check_item in check:
            union_field = str(check_item["emp_id"])+check_item["campus_name"]+check_item["create_date"]
            hash_value = hashlib.md5(union_field.encode("utf-8")).hexdigest()
            fields.append(hash_value)
        for hash_key in self.tmk_result:
            per_theme_data = self.tmk_result[hash_key][0]
            for field in per_theme_data:
                if type(per_theme_data[field]) == str and per_theme_data[field] !="null":
                    per_theme_data[field]="'"+per_theme_data[field]+"'"
            source_data = str(per_theme_data["emp_id"])+per_theme_data["campus_name"]+per_theme_data["create_date"]
            source_hash = hashlib.md5(source_data.encode("utf-8")).hexdigest()
            if (source_hash not in fields):
                conn.update(load_insert%per_theme_data)
            else:
                conn.update(load_update%per_theme_data)
        conn.commit()
        conn.close()
        self.tmk_result={}
