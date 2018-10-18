/*==============================================================*/
/* DBMS name:      MySQL 5.0                                    */
/* Created on:     2018/8/17 星期五 2:37:35                        */
/*==============================================================*/

drop table if exists bi_campus_show;

drop table if exists bi_sale_info;

drop table if exists bi_sale_show;

drop table if exists bi_sale_theme;

/*==============================================================*/
/* Table: bi_campus_show                                        */
/*==============================================================*/
create table bi_campus_show
(
   id                   int not null auto_increment,
   campus_id            int,
   campus_name          varchar(50),
   onjob_total          int comment '在岗人数总数',
   order_total          int comment '订单，即所有回单总数,本业务用回单(学生反馈)作为订单',
   order_valid_num      int comment '有效订单数,有效、再联系、邀约成功',
   order_invalid_num    int comment '参考order_valid_num',
   order_success_num    int comment '邀约成功数量',
   order_contact        int comment '再联系数量,邀约环节之一',
   order_parents        int comment '是否双亲陪同',
   create_date          date comment "订单创建日期",
   primary key (id)
);

/*==============================================================*/
/* Table: bi_sale_info                                          */
/*==============================================================*/
create table bi_sale_info
(
   id                   int not null auto_increment,
   emp_id               int,
   dept_id              int,
   dept_name            varchar(50),
   campus_id            int,
   campus_name          varchar(50),
   emp_en_name          varchar(15),
   emp_cn_name          varchar(15) comment '学员中文名',
   stu_sex              varchar(15) comment '学员性别',
   emp_status           varchar(10) comment '员工在职状态的中文释义，在/离职/试用等，依据实际而定',
   emp_state            int comment '员工在职状态，数字编码，1=在职，2=离职',
   student_id           int,
   create_time          timestamp comment '系统录入时间',
   update_time          timestamp comment '回单状态更改时间',
   old_to_new           varchar(50) comment '老单转新单',
   order_status         varchar(10) comment '回单有效性状态',
   state                int  comment '回单有效性状态编码',
   description          text comment '描述信息',
   invite_success_time  timestamp comment '在v_invitation2中，状态为邀请成功的时间戳',
   PRIMARY KEY (`id`),
   UNIQUE INDEX `index_vinston` (`student_id`)
);

alter table bi_sale_info comment '统计每个销售人员回单情况';

/*==============================================================*/
/* Table: bi_sale_show                                          */
/*==============================================================*/
create table bi_sale_show
(
   id                   int not null auto_increment,
   campus_id            int,
   campus_name          varchar(50) comment '员工所在校区的中文名字',
   emp_id               int,
   emp_name             varchar(50) comment '员工中文名字',
   order_total          int,
   order_valid_num      int comment '有效订单总数,含有效、再联系,邀约成功',
   order_invalid_num    int comment '参考order_valid_num,但不含未知订单',
   order_invite_num     int comment '成功邀约总数',
   order_contact_num    int comment '再联系总数',
   performance          int comment '业绩',
   isTarget             int comment '是否达标',
   begin_date           date comment '统计周期的开始时间，周期为天、周、月',
   end_date             date comment '统计周期的结束时间，周期为天、周、月',
   primary key (id)
);

/*==============================================================*/
/* Table: bi_sale_theme                                         */
/*==============================================================*/
create table bi_sale_theme
(

   id                   int not null auto_increment comment '自增主键id',
   campus_id            int,
   campus_name          varchar(50),
   emp_id               int not null,
   emp_name             varchar(50),
   order_total          int comment '所有录入系统的订单，即所有学生',
   order_valid_num      int comment '查询bi_sale_info表中order_status信息,为邀约成功，再联系，有效的总量,也可能为数字编号',
   order_invalid_num    int comment '所有bi_sale_info表中order_status为无效的回单总量',
   order_valid_rate     real comment '参考order_valid_num字段',
   order_invalid_rate   real comment '参考order_invalid_num字段',
   invite_avg_hours     int comment '对该员工成功邀请数量统计以后，计算每个邀请的平均耗时',
   circle_rate          real comment '员工订单数量，即回单的环比，即是比上个统计周期增加的百分比',
   invite_total         int comment '邀约成功的总数',
   contact_total        int comment '再联系总数,邀约环节中的一个',
   create_date          date comment "订单创建日期",
   order_no_parents     int comment '非双亲陪同的数量，计算方法是检测v_student表descript字段中是否有爷爷奶奶字样'
   valid_for_once       int comment '保存最初有效订单的数量,固定不变,尽管有效订单从有效变为无效',
   invalid_for_once       int comment '保存最初无效订单的数量,固定不变,尽管有效订单从无效变为有效',
   PRIMARY KEY (`id`),
   UNIQUE INDEX `index_vinston` (`emp_id`, `campus_name`, `create_date`)
);
