
SELECT v_invitation.student_id,sales_demo,v_invitation.s_d_time update_time,
        v_invitation.is_show,v_invitation.creator_id emp_id,user_name emp_name,
        department_id dept_id, v_student.create_time,v_invitation.visit_campus,
        v_invitation.validity,v_student.source_campus campus_name
FROM `v_invitation`
left join v_user on v_invitation.creator_id = v_user.user_id left join v_student on v_invitation.student_id = v_student.student_id
where (date(v_invitation.s_d_time) between  '2018-09-17' and '2018-10-16')
and v_user.department_id in(74,75,76)
and (sales_demo like '%第一次约访%')
and v_invitation.student_id in(select student_id from v_student where vreg_type not in('带到访','转介绍','公交站牌广告','W-In','C-In电话','个人渠道','陌拜数据') and is_deleted = 0 and owner_role_id > 0)     limit 0,1500
