


drop table if exists bi_contract_info;

drop table if exists bi_contract_theme;

CREATE TABLE `bi_contract_info` (
`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键自增id',
`contract_id` int(11) NULL COMMENT '合同id,对应多个课程',
`emp_id`      varchar(50) comment '创建该记录的员工的id,一般为合同签订者',
`emp_en_name` varchar(50) comment '学员英文名',
`emp_cn_name` varchar(50) comment '学员中文名',
`student_id` int(11) NULL,
`introduce_id` int(11) NULL COMMENT '课程介绍id，对应一个合同',
`campus_id` int(11) NULL COMMENT '校区id,v_teaching_campus映射校区名称',
`campus_name` varchar(50) NULL COMMENT '校区名称',
`sex` varchar(50) NULL,
`due_date` date NULL COMMENT '签约日期',
`type`   varchar(50) NULL COMMENT '合同所属类别',
PRIMARY KEY (`id`),
UNIQUE INDEX `index_contract` (`contract_id`, `student_id`, `introduce_id`)
);
