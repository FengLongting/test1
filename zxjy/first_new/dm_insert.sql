-- show databases ;
-- truncate table edu_dm.zx_dm;


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
-- 总有效咨询量
insert into edu_dm.zx_dm partition(dt)
select
    0 as customer_nums,
    count(distinct id) as clue_nums,
    null as time_type ,
    null as yearinfo ,
    null as monthinfo,
--     null as dayinfo,
--     null as hourinfo ,
    null as origin_type_state,
    null as clue_state_stat,
    'all' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id ,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwb.customer_clue_dwb1;
-- select count(*) from edu_dwb.customer_clue_dwb1;

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
--总咨询量 null 表示全部，all表示既有年，也有月
insert into edu_dm.zx_dm partition(dt)
select
    count(distinct customer_id) as customer_nums,
    0 as clue_nums,
    null as time_type ,
    null as yearinfo ,
    null as monthinfo,
--     null as dayinfo,
--     null as hourinfo ,
    null as origin_type_state,
    null as clue_state_stat,
    'all' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id ,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1;





-- -- 目的是增量存储
-- SET hive.exec.dynamic.partition=true;
-- SET hive.exec.dynamic.partition.mode=nonstrict;
-- set hive.exec.max.dynamic.partitions.pernode=10000;
-- set hive.exec.max.dynamic.partitions=100000;
-- set hive.exec.max.created.files=150000;
-- --hive压缩
-- set hive.exec.compress.intermediate=true;
-- set hive.exec.compress.output=true;
-- --写入时压缩生效
-- set hive.exec.orc.compression.strategy=COMPRESSION;
--
-- set mapreduce.map.speculative=false;
-- set mapreduce.reduce.speculative=false;
-- set hive.mapred.reduce.tasks.speculative.execution=false;
-- insert into edu_dm.zx_dm partition(dt)
-- select
--     0 as customer_nums,
--     count(distinct cc.id) as clue_nums,
--     case
-- --         when grouping__id =63
-- --         then 'hour'
-- --         when grouping__id =55
-- --         then 'day'
--         when grouping__id =51
--         then 'month'
--         when grouping__id =49
--         then 'year'
--     end time_type,
--     cr.yearinfo,
--     cr.monthinfo,
-- --     cr.dayinfo,
-- --     cr.hourinfo,
--     cr.origin_type_state,
--     cc.clue_state_stat,
--     'all' as group_type,
--     null as area,
--     null as itcast_subject_id,
--     null as subject_name,
--     null as itcast_school_id,
--     null as school_name,
--     null as tdepart_id,
--     null as tdepart_name,
--     '2022-07-02' as dt
-- from edu_dwb.customer_clue_dwb1 cc
-- left join edu_dwd.fact_customer_relationship1 cr
-- on cr.id=cc.customer_relationship_id and cr.end_date='9999-99-99'
-- group by cr.yearinfo,cr.monthinfo,cr.origin_type_state,cc.clue_state_stat
-- grouping sets
-- (
-- (cr.yearinfo,cr.origin_type_state,cc.clue_state_stat),  --49
-- (cr.monthinfo,cr.yearinfo,cr.origin_type_state,cc.clue_state_stat) --51
-- );


--55

-- 第一个

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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,

    'year' as time_type,

    cr.yearinfo,
    null as monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
--     case when grouping__id in(113,115)
--         then 'c.area'
--         when grouping__id in(433,435)
--         then 'subject'
--         when grouping__id in(1585,1587)
--         then 'school'
--         when grouping__id in(6193,6195)
--         then 'tdepart'
    'all' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.yearinfo,cr.origin_type_state,cc.clue_state_stat;


-- 第二个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,

    'year' as time_type,

    cr.yearinfo,
    null as monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
--     case when grouping__id in(113,115)
--         then 'c.area'
--         when grouping__id in(433,435)
--         then 'subject'
--         when grouping__id in(1585,1587)
--         then 'school'
--         when grouping__id in(6193,6195)
--         then 'tdepart'
    'c.area' as group_type,
    c.area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.yearinfo,cr.origin_type_state,cc.clue_state_stat,c.area;




-- 第三个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,

    'year' as time_type,

    cr.yearinfo,
    null as monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
--     case when grouping__id in(113,115)
--         then 'c.area'
--         when grouping__id in(433,435)
--         then 'subject'
--         when grouping__id in(1585,1587)
--         then 'school'
--         when grouping__id in(6193,6195)
--         then 'tdepart'
    'subject' as group_type,
    null as area,
    cr.itcast_subject_id,
    sub.name as subject_name,
    null as itcast_school_id,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.yearinfo,cr.origin_type_state,cc.clue_state_stat,cr.itcast_subject_id,sub.name;

-- 第四个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,

    'year' as time_type,

    cr.yearinfo,
    null as monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
--     case when grouping__id in(113,115)
--         then 'c.area'
--         when grouping__id in(433,435)
--         then 'subject'
--         when grouping__id in(1585,1587)
--         then 'school'
--         when grouping__id in(6193,6195)
--         then 'tdepart'
    'school' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    cr.itcast_school_id,
    sch.name as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.yearinfo,cr.origin_type_state,cc.clue_state_stat,cr.itcast_school_id,sch.name;



-- 第五个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,

    'year' as time_type,

    cr.yearinfo,
    null as monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
    'tdepart' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id,
    null as school_name,
    e.tdepart_id,
    dep.name as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.yearinfo,cr.origin_type_state,cc.clue_state_stat,e.tdepart_id,dep.name;















-----------------------------------------------------------------------------------

-- 第一个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,
    'month' as time_type,
    cr.yearinfo,
    cr.monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
--     case when grouping__id in(113,115)
--         then 'c.area'
--         when grouping__id in(433,435)
--         then 'subject'
--         when grouping__id in(1585,1587)
--         then 'school'
--         when grouping__id in(6193,6195)
--         then 'tdepart'
    'all' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.monthinfo,cr.yearinfo,cr.origin_type_state,cc.clue_state_stat;

-- select * from edu_dm.zx_dm where customer_nums = 13503;





-- 第二个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,
    'month' as time_type,
    cr.yearinfo,
    cr.monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
--     case when grouping__id in(113,115)
--         then 'c.area'
--         when grouping__id in(433,435)
--         then 'subject'
--         when grouping__id in(1585,1587)
--         then 'school'
--         when grouping__id in(6193,6195)
--         then 'tdepart'
    'c.area' as group_type,
    c.area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.monthinfo,cr.yearinfo,cr.origin_type_state,cc.clue_state_stat,c.area;

select * from edu_dm.zx_dm where customer_nums = 41;



-- 第三个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,
    'month' as time_type,
    cr.yearinfo,
    cr.monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
--     case when grouping__id in(113,115)
--         then 'c.area'
--         when grouping__id in(433,435)
--         then 'subject'
--         when grouping__id in(1585,1587)
--         then 'school'
--         when grouping__id in(6193,6195)
--         then 'tdepart'
    'subject' as group_type,
    null as area,
    cr.itcast_subject_id,
    sub.name as subject_name,
    null as itcast_school_id,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.monthinfo,cr.yearinfo,cr.origin_type_state,cc.clue_state_stat,cr.itcast_subject_id,sub.name;


-- 第四个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,
    'month' as time_type,
    cr.yearinfo,
    cr.monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
--     case when grouping__id in(113,115)
--         then 'c.area'
--         when grouping__id in(433,435)
--         then 'subject'
--         when grouping__id in(1585,1587)
--         then 'school'
--         when grouping__id in(6193,6195)
--         then 'tdepart'
    'school' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    cr.itcast_school_id,
    sch.name as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.monthinfo,cr.yearinfo,cr.origin_type_state,cc.clue_state_stat,cr.itcast_school_id,sch.name;



-- 第五个
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
insert into edu_dm.zx_dm partition(dt)
select
-- count(1)
    count(distinct cr.customer_id) as customer_nums,
    0 as clue_nums,
    'month' as time_type,
    cr.yearinfo,
    cr.monthinfo,
    cr.origin_type_state, --'数据来源,线上1,线下0'
    cc.clue_state_stat,   --'线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据'
    'tdepart' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id,
    null as school_name,
    e.tdepart_id,
    dep.name as tdepart_name,
    '2022-07-02' as dt
from edu_dwd.fact_customer_relationship1 cr
left JOIN edu_dwd.fact_customer_clue1 cc on cr.id=cc.customer_relationship_id and cc.end_date='9999-99-99'
left JOIN edu_dwd.dim_customer1 c on c.id=cr.customer_id
left join edu_dwd.dim_itcast_subject sub on sub.id=cr.itcast_subject_id
left join edu_dwd.dim_itcast_school sch on sch.id=cr.itcast_school_id
left join edu_dwd.dim_employee1 e on e.id=cc.creator
left join edu_dwd.dim_scrm_department1 dep on dep.id=e.tdepart_id
group by cr.monthinfo,cr.yearinfo,cr.origin_type_state,cc.clue_state_stat,e.tdepart_id,dep.name;



-- select count(*) from edu_dm.zx_dm limit 1000;


































-- 咨询表
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
insert into edu_dm.zx_dm partition(dt)
select
    0 as customer_nums,
    count(distinct cc.id) as clue_nums,
    'all' as time_type,
    cr.yearinfo,
    cr.monthinfo,
--     cr.dayinfo,
--     cr.hourinfo,
    cr.origin_type_state,
    cc.clue_state_stat,
    'all' as group_type,
    null as area,
    null as itcast_subject_id,
    null as subject_name,
    null as itcast_school_id,
    null as school_name,
    null as tdepart_id,
    null as tdepart_name,
    '2022-07-02' as dt
from edu_dwb.customer_clue_dwb1 cc
left join edu_dwd.fact_customer_relationship1 cr
on cr.id=cc.customer_relationship_id and cr.end_date='9999-99-99'
group by cr.yearinfo,cr.monthinfo,cr.origin_type_state,cc.clue_state_stat;



