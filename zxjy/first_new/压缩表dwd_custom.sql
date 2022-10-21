CREATE TABLE IF NOT EXISTS edu_dwd.dim_customer1 (
id int COMMENT 'key id',
--   customer_relationship_id int COMMENT '当前意向id',
--   create_date_time STRING COMMENT '创建时间',
--   update_date_time STRING COMMENT '最后更新时间',
--   deleted int  COMMENT '是否被删除（禁用）',
--   name STRING COMMENT '姓名',
--   idcard STRING  COMMENT '身份证号',
--   birth_year int COMMENT '出生年份',
--   gender STRING COMMENT '性别',
--   phone STRING COMMENT '手机号',
--   wechat STRING COMMENT '微信',
--   qq STRING COMMENT 'qq号',
--   email STRING COMMENT '邮箱',
area STRING COMMENT '所在区域'
--   leave_school_date date COMMENT '离校时间',
--   graduation_date date COMMENT '毕业时间',
--   bxg_student_id STRING COMMENT '博学谷学员ID，可能未关联到，不存在',
--   creator int COMMENT '创建人ID',
--   origin_type STRING COMMENT '数据来源',
--   origin_channel STRING COMMENT '来源渠道',
--   tenant int,
--   md_id int COMMENT '中台id'
)comment '客户表'
PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');






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
INSERT into edu_dwd.dim_customer1 partition(dt)
SELECT
--     (
id,
--     customer_relationship_id integer,
--     create_date_time         varchar(max
-- ) ,
-- 	update_date_time STRING,
-- 	deleted integer,
-- 	name STRING,
-- 	idcard STRING,
-- 	birth_year integer,
-- 	gender STRING,
-- 	phone STRING,
-- 	wechat STRING,
-- 	qq STRING,
-- 	email STRING,
area,
-- 	leave_school_date date(14),
-- 	graduation_date date(14),
-- 	bxg_student_id STRING,
-- 	creator integer,
-- 	origin_type STRING,
-- 	origin_channel STRING,
-- 	tenant integer,
-- 	md_id integer,
start_time as dt
from edu_ods.customer;