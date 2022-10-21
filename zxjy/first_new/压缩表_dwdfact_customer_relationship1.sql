DROP TABLE edu_dwd.fact_customer_relationship1;
CREATE TABLE IF NOT EXISTS edu_dwd.fact_customer_relationship1 (
id int COMMENT '客户关系id',
--   create_date_time STRING COMMENT '创建时间',
--   update_date_time STRING COMMENT '最后更新时间',
--   deleted string COMMENT '是否被删除（禁用）',
customer_id int COMMENT '所属客户id',
--   first_id int COMMENT '第一条客户关系id',
--   belonger int COMMENT '归属人',
--   belonger_name STRING COMMENT '归属人姓名',
--   initial_belonger int COMMENT '初始归属人',
--   distribution_handler int COMMENT '分配处理人',
--   business_scrm_department_id int COMMENT '归属部门',
--   last_visit_time STRING COMMENT '最后回访时间',
--   next_visit_time STRING COMMENT '下次回访时间',
--   origin_type STRING COMMENT '数据来源',
itcast_school_id int COMMENT '校区Id',
itcast_subject_id int COMMENT '学科Id',
--   intention_study_type STRING COMMENT '意向学习方式',
--   anticipat_signup_date STRING COMMENT '预计报名时间',
--   level STRING COMMENT '客户级别',
--   creator int COMMENT '创建人',
--   current_creator int COMMENT '当前创建人：初始==创建人，当在公海拉回时为 拉回人',
--   creator_name STRING COMMENT '创建者姓名',
--   origin_channel STRING COMMENT '来源渠道',
--   comment STRING COMMENT '备注',
--   first_customer_clue_id int COMMENT '第一条线索id',
--   last_customer_clue_id int COMMENT '最后一条线索id',
--   process_state STRING COMMENT '处理状态',
--   process_time STRING COMMENT '处理状态变动时间',
--   payment_state STRING COMMENT '支付状态',
--   payment_time STRING COMMENT '支付状态变动时间',
--   signup_state STRING COMMENT '报名状态',
--   signup_time STRING COMMENT '报名时间',
--   notice_state STRING COMMENT '通知状态',
--   notice_time STRING COMMENT '通知状态变动时间',
--   lock_state STRING COMMENT '锁定状态',
--   lock_time STRING COMMENT '锁定状态修改时间',
--   itcast_clazz_id int COMMENT '所属ems班级id',
--   itcast_clazz_time STRING COMMENT '报班时间',
--   payment_url STRING COMMENT '付款链接',
--   payment_url_time STRING COMMENT '支付链接生成时间',
--   ems_student_id int COMMENT 'ems的学生id',
--   delete_reason STRING COMMENT '删除原因',
--   deleter int COMMENT '删除人',
--   deleter_name STRING COMMENT '删除人姓名',
--   delete_time STRING COMMENT '删除时间',
--   course_id int COMMENT '课程ID',
--   course_name STRING COMMENT '课程名称',
--   delete_comment STRING COMMENT '删除原因说明',
--   close_state STRING COMMENT '关闭装填',
--   close_time STRING COMMENT '关闭状态变动时间',
--   appeal_id int COMMENT '申诉id',
--   tenant int COMMENT '租户',
--   total_fee DECIMAL COMMENT '报名费总金额',
--   belonged int COMMENT '小周期归属人',
--   belonged_time STRING COMMENT '归属时间',
--   belonger_time STRING COMMENT '归属时间',
--   transfer int COMMENT '转移人',
--   transfer_time STRING COMMENT '转移时间',
--   follow_type int COMMENT '分配类型，0-自动分配，1-手动分配，2-自动转移，3-手动单个转移，4-手动批量转移，5-公海领取',
--   transfer_bxg_oa_account STRING COMMENT '转移到博学谷归属人OA账号',
--   transfer_bxg_belonger_name STRING COMMENT '转移到博学谷归属人OA姓名',
origin_type_state STRING COMMENT '数据来源,线上1,线下0',
--   yearinfo string comment '年',
--   monthinfo string comment '月',
--   hourinfo string comment '小时',
--   day_info string comment '天',
end_date STRING comment '有效时间'
)comment '客户关系表'
PARTITIONED BY(yearinfo STRING,monthinfo STRING)
CLUSTERED BY (id) sorted by (id) into 10 buckets
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');











-- truncate table edu_dwd.fact_customer_relationship1;





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

INSERT into edu_dwd.fact_customer_relationship1 partition(yearinfo,monthinfo)
SELECT
id,

--     create_date_time,
--     update_date_time,
--     deleted,
    customer_id,
--     first_id,
--     belonger,
--     belonger_name,
--     initial_belonger,
--     distribution_handler,
--     business_scrm_department_id,
--     last_visit_time,
--     next_visit_time,
--     origin_type,
case when itcast_school_id=0 or itcast_school_id is null
    then -1
    else itcast_school_id
end itcast_school_id,
case when itcast_subject_id=0 or itcast_subject_id is null
    then -1
    else itcast_subject_id
end itcast_subject_id,
--     intention_study_type,
--     anticipat_signup_date,
--     level,
--     creator,
--     current_creator,
--     creator_name,
--     origin_channel,
--     comment,
--     first_customer_clue_id,
--     last_customer_clue_id,
--     process_state,
--     process_time,
--     payment_state,
--     payment_time,
--     signup_state,
--     signup_time,
--     notice_state,
--     notice_time,
--     lock_state,
--     lock_time,
--     itcast_clazz_id,
--     itcast_clazz_time,
--     payment_url,
--     payment_url_time,
--     ems_student_id,
--     delete_reason,
--     deleter,
--     deleter_name,
--     delete_time,
--     course_id,
--     course_name,
--     delete_comment,
--     close_state,
--     close_time,
--     appeal_id,
--     tenant,
--     total_fee,
--     belonged,
--     belonged_time,
--     belonger_time,
--     transfer,
--     transfer_time,
--     follow_type,
--     transfer_bxg_oa_account,
--     transfer_bxg_belonger_name,
case when origin_type IN ('NETSERVICE', 'PRESIGNUP')
     then 1
     else 0
end origin_type_state,
'9999-99-99' as end_date,
substr(create_date_time,1,4) as yearinfo,
substr(create_date_time,1,7) as monthinfo
from edu_ods.customer_relationship ;



-- select
-- substr(create_date_time,1,7) as monthinfo
-- from edu_ods.customer_relationship ;
