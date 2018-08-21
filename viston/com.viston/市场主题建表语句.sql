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
   onjob_total          int,
   order_total          int,
   order_valid_num      int,
   order_invalid_num    int,
   order_success_num    int,
   order_contact        int,
   order_parents        int comment '是否双亲陪同',
   begin                date,
   end                  date,
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
   emp_cn_name          varchar(15),
   stu_sex              varchar(15) comment '学员性别',
   emp_status           varchar(10) comment '员工在职状态，中文释义',
   emp_state            int         comment '员工在职状态，数字编码，1=在职，2=离职',
   student_id           int,
   create_time          timestamp comment '系统录入时间',
   order_status         varchar(10) comment '回单有效性状态',
   state                int        comment '回单有效性状态编码',
   invite_success_time  timestamp comment '在v_invitation2中，状态为邀请成功的时间戳',
   primary key (id)
);

alter table bi_sale_info comment '统计每个销售人员回单情况';

/*==============================================================*/
/* Table: bi_sale_show                                          */
/*==============================================================*/
create table bi_sale_show
(
   id                   int not null auto_increment,
   campus_id            int,
   campus_name          varchar(50),
   emp_id               int,
   emp_name             varchar(50),
   order_total          int,
   order_valid_num      int,
   order_invalid_num    int,
   order_invite_num     int,
   order_contact_num    int,
   performance          int,
   isTarget             int,
   begin                date,
   end                  date,
   primary key (id)
);

/*==============================================================*/
/* Table: bi_sale_theme                                         */
/*==============================================================*/
create table bi_sale_theme
(
   id                   int not null auto_increment,
   campus_id            int,
   campus_name          varchar(50),
   emp_id               int,
   emp_name             varchar(50),
   order_total          int comment '所有录入系统的订单，即所有学生',
   order_valid_num      int comment '查询销售人员信息表中order_status信息,为邀约成功，再联系，有效的总量,也可能为数字编号',
   order_invalid_num    int comment '所有销售人员信息表中order_status为无效的回单总量',
   order_valid_rate     real,
   order_invalid_rage   real,
   invite_avg_hours     int,
   circle_rate          real,
   invite_total         int,
   contact_total        int,
   begin_date           date,
   end_date             date,
   primary key (id)
);
