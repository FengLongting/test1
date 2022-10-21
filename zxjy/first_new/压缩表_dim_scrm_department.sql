-- auto-generated definition
show databases ;
use edu_dwd;
show tables ;

create table if not exists edu_dwd.dim_scrm_department1
(
    id   int comment '部门id',
    name string comment '部门名称'
)comment '部门表'
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
insert into edu_dwd.dim_scrm_department1 partition (dt)
select
    id      ,
    name    ,
    start_time as dt
from edu_ods.scrm_department


