# drop DATABASE zxjy ;
CREATE DATABASE zxjy;
use zxjy;
drop table zxjy_result;
create table zxjy_result(
    customer_nums     int comment '意向客户总和',
    clue_nums         int comment '有效线索总和',
    time_type         varchar(50) comment '时间分组',
    yearinfo          varchar(50) comment '年',
    monthinfo         varchar(50) comment '月',
    origin_type_state varchar(50) comment '数据来源:0.线下；1.线上',
    clue_state_stat   varchar(50) comment '新老客户：0.老客户；1.新客户',
    group_type        varchar(50) comment '其他',
    area              varchar(50) comment '所在区域',
    itcast_subect_id  int comment '学科Id',
    subject_name      varchar(50) comment '学科名称',
    itcast_school_id  int comment '校区Id',
    school_name       varchar(50) comment '校区名称',
    tdepart_id        int comment '直属部门id',
    tdepart_name      varchar(50) comment '部门名称',
    dt                varchar(50)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;