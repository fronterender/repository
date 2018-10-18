
drop table if exists bi_visit_info;
drop table if exists bi_visit_theme;

CREATE TABLE `bi_visit_info` (
`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键自增id',
`student_id` int(11) NULL COMMENT '学生id',
`visit_campus` varchar(50) NULL COMMENT '学生到访的校区',
`visit_time`   datetime null COMMENT "学生到访给时间",
`invite_role_id`int(11) NULL COMMENT '邀请者id,0表示自己来的',
`department_name` varchar(50) NULL COMMENT '邀请者所在的部门名称',
`v_ftm_id`   varchar(255) NULL COMMENT '跟进人id,邀约成功以后介入,负责到访跟进落实',
PRIMARY KEY (`id`),
UNIQUE INDEX `index_visit` (`student_id`, `visit_campus`)
);

CREATE TABLE `bi_visit_theme` (
`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键自增id',
`visit_campus` varchar(50) NULL COMMENT '学生到访的校区',
`visit_date`   date null COMMENT "学生到访给时间",
`visit_num` int(11) NULL COMMENT '一个校区某天到访的人数',
PRIMARY KEY (`id`),
UNIQUE INDEX `index_visit` (`visit_date`, `visit_campus`)
);
