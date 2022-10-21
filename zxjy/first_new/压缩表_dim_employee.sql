drop table edu_dwd.dim_employee1;
create TABLE IF NOT EXISTS edu_dwd.dim_employee1
(
id                  int       COMMENT '员工id',
--     email               string    comment '公司邮箱，OA登录账号',
--     real_name           string    comment '员工的真实姓名',
--     phone               string    comment '手机号，目前还没有使用；隐私问题OA接口没有提供这个属性，',
-- department_id       string    comment 'OA中的部门编号，有负值'
--     department_name     string    comment 'OA中的部门名',
--     remote_login        tinyint   comment '员工是否可以远程登录',
--     job_number          string    comment '员工工号',
--     cross_school        tinyint   comment '是否有跨校区权限',
--     last_login_date     string  comment '最后登录日期',
--     creator             int       comment '创建人',
--     create_date_time    string  comment '创建时间',
--     update_date_time    string comment '最后更新时间',
--     deleted             tinyint   comment '是否被删除（禁用）',
--     scrm_department_id  tinyint   comment 'SCRM内部部门id',
--     leave_office        tinyint   comment '离职状态',
--     leave_office_time   string  comment '离职时间',
--     reinstated_time     string  comment '复职时间',
--     superior_leaders_id int       comment '上级领导ID',
    tdepart_id          int       comment '直属部门'
--     tenant              int COMMENT '租户',
--     ems_user_name       string
)comment '员工信息表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compress'='SNAPPY');



SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;

set mapreduce.map.speculative=false;
set mapreduce.reduce.speculative=false;
set hive.mapred.reduce.tasks.speculative.execution=false;
INSERT into edu_dwd.dim_employee1 partition(dt)
SELECT
id,
--     email               string    comment '公司邮箱，OA登录账号',
--     real_name           string    comment '员工的真实姓名',
--     phone               string    comment '手机号，目前还没有使用；隐私问题OA接口没有提供这个属性，',
-- department_id,
--     department_name     string    comment 'OA中的部门名',
--     remote_login        tinyint   comment '员工是否可以远程登录',
--     job_number          string    comment '员工工号',
--     cross_school        tinyint   comment '是否有跨校区权限',
--     last_login_date     string  comment '最后登录日期',
--     creator             int       comment '创建人',
--     create_date_time    string  comment '创建时间',
--     update_date_time    string comment '最后更新时间',
--     deleted             tinyint   comment '是否被删除（禁用）',
--     scrm_department_id  tinyint   comment 'SCRM内部部门id',
--     leave_office        tinyint   comment '离职状态',
--     leave_office_time   string  comment '离职时间',
--     reinstated_time     string  comment '复职时间',
--     superior_leaders_id int       comment '上级领导ID',
    tdepart_id,
--     tenant              int COMMENT '租户',
--     ems_user_name       string,
start_time as dt
from edu_ods.employee;