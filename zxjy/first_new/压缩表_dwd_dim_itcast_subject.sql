CREATE TABLE IF NOT EXISTS edu_dwd.dim_scrm_department1 (
id int COMMENT '部门id',
name string COMMENT '部门名称'
--   parent_id int COMMENT '父部门id',
--   create_date_time string COMMENT '创建时间',
--   update_date_time string COMMENT '更新时间',
--   deleted string COMMENT '删除标志',
--   id_path string COMMENT '编码全路径',
--   tdepart_code int COMMENT '直属部门',
--   creator string COMMENT '创建者',
--   depart_level int COMMENT '部门层级',
--   depart_sign int COMMENT '部门标志，暂时默认1',
--   depart_line int COMMENT '业务线，存储业务线编码',
--   depart_sort int COMMENT '排序字段',
--   disable_flag int COMMENT '禁用标志',
--   tenant int COMMENT '租户'
) comment '部门表'
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
INSERT into edu_dwd.dim_scrm_department1 partition(dt)
SELECT
id,
name,
--   parent_id int COMMENT '父部门id',
--   create_date_time string COMMENT '创建时间',
--   update_date_time string COMMENT '更新时间',
--   deleted string COMMENT '删除标志',
--   id_path string COMMENT '编码全路径',
--   tdepart_code int COMMENT '直属部门',
--   creator string COMMENT '创建者',
--   depart_level int COMMENT '部门层级',
--   depart_sign int COMMENT '部门标志，暂时默认1',
--   depart_line int COMMENT '业务线，存储业务线编码',
--   depart_sort int COMMENT '排序字段',
--   disable_flag int COMMENT '禁用标志',
start_time as dt
from edu_ods.scrm_department;