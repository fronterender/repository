import QueryData as QueryData
status_dict = {'未拨打':1,'未知':2,'有效':3,'邀约成功':4,'无效':5,'再联系':6,'未知再联系':7}
sql = "select creator_role_id,user_dept,source_campus,\
             u.name,u.user_name,u.status,s.student_id,\
             s.sex,s.create_time,s.validity\
      from v_student_2 as s left join v_user as u\
      on s.creator_role_id = u.user_id"
class Transform():
    # 存放数据库查询结果
    result = []
    def __init__(self):
        pass
    def extract(self):
        queryData = QueryData.QueryData()
        try:
            queryData.connect()
            result = queryData.query(sql)
            queryData.close()
            self.result = result
        except Exception as e:
            raise
    def transform(self):
        # for entry in result:
        #     proterty = {}
        #     proterty[]
    def load(self):
        pass
