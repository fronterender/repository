drop table if exists bi_tmk_info;
drop table if exists bi_tmk_theme;
CREATE TABLE `bi_tmk_info` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `emp_id` int NULL COMMENT '员工编号',
  `emp_name` varchar(50) NULL COMMENT '员工姓名',
  `dept_id` int NULL COMMENT '部门名称',
  `group_name` varchar(50) NULL COMMENT '组别,成人组，少儿组',
  `student_id` int NULL COMMENT '学生id',
  `campus_name` varchar(50) NULL COMMENT '校区名称',
  `create_time` datetime NULL  COMMENT '订单创建时间',
  `visit_campus` varchar(50) NULL COMMENT '到访校区',
  `validity` varchar(50) NULL COMMENT '订单状态',
  PRIMARY KEY (`id`),
  UNIQUE INDEX `index_vinston` (`emp_id`, `campus_name`, `create_time`)
);
CREATE TABLE `bi_tmk_theme` (
`id` int NOT NULL  AUTO_INCREMENT COMMENT '自增主键id',
`emp_id` int NULL COMMENT '员工id',
`emp_name` varchar(50) NULL COMMENT '员工姓名',
`group_name` varchar(50) NULL COMMENT '员工组别',
`order_total` int NULL COMMENT '新单，通常是昨天新增，昨天之前为旧单，新单也可能为某个时间点的，之前的为旧',
`campus_name` varchar(50) NULL COMMENT '校区名称',
`invite_total` int NULL COMMENT '邀请成功总量',
`contact_total` int NULL COMMENT 'validity字段为再联系总量',
`order_invalid_num` int NULL COMMENT 'validity字段为无效的总量',
`order_unknown_num` int NULL COMMENT 'v_invitation表中validity中为未知的订单总数',
`create_date` date NULL COMMENT '订单创建日期',
 PRIMARY KEY (`id`),
 UNIQUE INDEX `index_vinston` (`emp_id`, `campus_name`, `create_date`)
);
