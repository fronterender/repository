select creator_role_id,user_dept,source_campus,
        u.name,u.user_name,u.status,
        s.student_id,s.sex,s.create_time,s.validity
from v_student_2 as s left join v_user as u
on s.creator_role_id = u.user_id


SELECT * from v_student_2 where create_time between "2018-08-28 00:00:00" and "2018-08-28 23:59:59"
and vreg_type not in ('带到访','转介绍','公交站牌广告','W-In','个人渠道','online-在线咨询','online-网站留言','online-大众点评','online-推广来电','online-58同城','online-离线宝','online-其它','online-手机抓取','online-异业合作','online-朋友圈','online-W-In','online-今日头条','商家岛微信砍价','商家岛微信砍价2')
and source_campus="富兴中心"
and is_deleted =0
and owner_role_id > 0
insert into bi_sale_theme(
                   emp_id,
                   campus_name,
                   emp_name,
                   order_total,
                    order_valid_num,
                    order_invalid_num,
                    invite_total,contact_total,create_date  )
          values(6190,'梅溪新天地中心','陈佩琳',16,4,8,2,2,'2018-08-20',)
insert into bi_sale_info(
                emp_id,campus_name,emp_en_name,
                emp_cn_name,stu_sex,emp_status,emp_state,
                student_id,create_time, order_status,state,update_time)
                values( 6190,'梅溪新天地中心','aprilchen', '陈佩琳','女','在职',1,
                  1015097,'2018-08-20 00:07:28','未知',2,'2018-08-25 18:42:11')


  SELECT v_invitation_2.student_id,v_invitation_2.creator_id emp_id,user_name emp_name,department_id dept_id, v_student_2.create_time,v_invitation_2.visit_campus,v_invitation_2.validity,v_student_2.source_campus campus_name
  FROM `v_invitation_2` left join v_user on v_invitation_2.creator_id = v_user.user_id left join v_student_2 on v_invitation_2.student_id = v_student_2.student_id
  where v_invitation_2.is_show = 0 and v_invitation_2.student_id in(select student_id from v_student_2 where vreg_type not in('带到访','转介绍','公交站牌广告','W-In','C-In电话','个人渠道','陌拜数据') and is_deleted = 0 and  owner_role_id > 0)

insert into tmk_info (
                    emp_id,emp_name,dept_id,group_name,student_id,
                    campus_name,visit_campus,validity,create_time)
          values(6401,'文洁茹',28,null,1015097,'梅溪新天地中心',null,'未知','2018-08-20 00:07:28')

SELECT v_invitation.student_id,v_invitation.creator_id emp_id,user_name emp_name,
      department_id dept_id, v_student.create_time,v_invitation.visit_campus,v_invitation.validity,
      v_student.source_campus campus_name
FROM `v_invitation` left join v_user on v_invitation.creator_id = v_user.user_id left join v_student on v_invitation.student_id = v_student.student_id
where v_user.department_id in(74,75,76) and v_invitation.is_show = 0 and v_invitation.student_id in(select student_id from v_student where vreg_type not in('带到访','转介绍','公交站牌广告','W-In','C-In电话','个人渠道','陌拜数据') and is_deleted = 0 and  owner_role_id > 0)    limit 0,700

select creator_role_id,user_dept,source_campus,u.name,u.user_name,u.status,
        s.student_id,s.sex,s.create_time,s.update_time,s.validity
from v_student_2 as s left join v_user as u on s.creator_role_id = u.user_id
where date(s.create_time) = date_sub(curdate(),interval 1 day) and vreg_type not in ('带到访','转介绍','公交站牌广告','W-In','个人渠道','online-在线咨询','online-网站留言','online-大众点评','online-推广来电','online-58同城','online-离线宝','online-其它','online-手机抓取','online-异业合作','online-朋友圈','online-W-In','online-今日头条','商家岛微信砍价','商家岛微信砍价2') and is_deleted =0 and owner_role_id > 0

select * from
(SELECT * from vinston_unit_practice where `u_id` = 12 AND `status` = 1 AND `first_id` = 205 and update_time  is not null  order by update_time desc) vin
GROUP BY exercise_design_id
