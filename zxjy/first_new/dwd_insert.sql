-- dwd层插入数据
-- 设置动态分区参数
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
-- TRUNCATE TABLE edu_dwd.fact_customer_relationship;
INSERT INTO edu_dwd.fact_customer_relationship partition(yearinfo,monthinfo)
SELECT
    id,
    create_date_time,
    update_date_time,
    deleted,
    customer_id,
    first_id,
    belonger,
    belonger_name,
    initial_belonger,
    distribution_handler,
    business_scrm_department_id,
    last_visit_time,
    next_visit_time,
    origin_type,
    case when itcast_school_id=0 or itcast_school_id is null
        then -1
        else itcast_school_id
    end itcast_school_id,
    case when itcast_subject_id=0 or itcast_subject_id is null
        then -1
        else itcast_subject_id
    end itcast_subject_id,
    intention_study_type,
    anticipat_signup_date,
    level,
    creator,
    current_creator,
    creator_name,
    origin_channel,
    comment,
    first_customer_clue_id,
    last_customer_clue_id,
    process_state,
    process_time,
    payment_state,
    payment_time,
    signup_state,
    signup_time,
    notice_state,
    notice_time,
    lock_state,
    lock_time,
    itcast_clazz_id,
    itcast_clazz_time,
    payment_url,
    payment_url_time,
    ems_student_id,
    delete_reason,
    deleter,
    deleter_name,
    delete_time,
    course_id,
    course_name,
    delete_comment,
    close_state,
    close_time,
    appeal_id,
    tenant,
    total_fee,
    belonged,
    belonged_time,
    belonger_time,
    transfer,
    transfer_time,
    follow_type,
    transfer_bxg_oa_account,
    transfer_bxg_belonger_name,
    case when origin_type IN ('NETSERVICE', 'PRESIGNUP')
         then 1
         else 0
    end origin_type_state,
    '9999-99-99' as end_date,
    substr(create_date_time,1,4) as yearinfo,
    substr(create_date_time,1,7) as monthinfo
--     substr(create_date_time,1,13) as hourinfo,
--     substr(create_date_time,1,10) as dayinfo
from edu_ods.customer_relationship where deleted=0 ;




--线索表customer_clue
INSERT into edu_dwd.fact_customer_clue partition(start_date)
SELECT
    id,
    create_date_time,
    update_date_time,
    deleted,
    customer_id,
    customer_relationship_id,
    session_id,
    sid,
    status,
    'users',
    create_time,
    platform,
    s_name,
    seo_source,
    seo_keywords,
    ip,
    referrer,
    from_url,
    landing_page_url,
    url_title,
    to_peer,
    manual_time,
    begin_time,
    reply_msg_count,
    total_msg_count,
    msg_count,
    comment,
    finish_reason,
    finish_user,
    end_time,
    platform_description,
    browser_name,
    os_info,
    area,
    country,
    province,
    city,
    creator,
    name,
    idcard,
    phone,
    itcast_school_id,
    itcast_school,
    itcast_subject_id,
    itcast_subject,
    wechat,
    qq,
    email,
    gender,
    level,
    origin_type,
    information_way,
    working_years,
    technical_directions,
    customer_state,
    valid,
    anticipat_signup_date,
    clue_state,
    scrm_department_id,
    superior_url,
    superior_source,
    landing_url,
    landing_source,
    info_url,
    info_source,
    origin_channel,
    course_id,
    course_name,
    zhuge_session_id,
    is_repeat,
    tenant,
    activity_id,
    activity_name,
    follow_type,
    shunt_mode_id,
    shunt_employee_group_id,
    case when clue_state='VALID_NEW_CLUES'
        then 1
        when clue_state='VALID_PUBLIC_NEW_CLUE'
        then 0
        else -1
    end clue_state_stat,
    '9999-99-99' as end_date,
    if(update_date_time is null or update_date_time='',substr(create_date_time,1,10),substr(update_date_time,1,10)) as start_date
from edu_ods.customer_clue where deleted = 'false';

-- 客户表
INSERT into edu_dwd.dim_customer partition(dt)
SELECT
    *
from edu_ods.customer;

--学科表
insert into edu_dwd.dim_itcast_subject partition(dt)
select
    case when id=0 or id is null
        then -1
        else id
    end id,
    create_date_time,
    update_date_time,
    deleted,
    name,
    code,
    tenant,
    start_time as dt
from edu_ods.itcast_subject;

--itcast_school校区表
insert into edu_dwd.dim_itcast_school partition(dt)
select
    case when id=0 or id is null
        then -1
        else id
    end id,
    create_date_time,
    update_date_time,
    deleted,
    name,
    code,
    tenant,
    start_time as dt
from edu_ods.itcast_school;

--部门表
INSERT into edu_dwd.dim_scrm_department partition(dt)
SELECT
    *
from edu_ods.scrm_department;

--员工表
INSERT into edu_dwd.dim_employee partition(dt)
SELECT
    *
from edu_ods.employee;

--线索申诉表
insert into edu_dwd.fact_customer_appeal partition(dt)
select
    *
from edu_ods.customer_appeal;














-- edu_dwb层建表
-- 有效线索宽表
create table edu_dwb.customer_clue_dwb(
  id int COMMENT 'customer_clue_id',
  create_date_time STRING COMMENT '创建时间',
  update_date_time STRING COMMENT '最后更新时间',
  deleted STRING COMMENT '是否被删除（禁用）',
  customer_id int COMMENT '客户id',
  customer_relationship_id int COMMENT '客户关系id',
  session_id STRING COMMENT '七陌会话id',
  sid STRING COMMENT '访客id',
  status STRING COMMENT '状态（undeal待领取 deal 已领取 finish 已关闭 changePeer 已流转）',
  users STRING COMMENT '所属坐席',
  create_time STRING COMMENT '七陌创建时间',
  platform STRING COMMENT '平台来源 （pc-网站咨询|wap-wap咨询|sdk-app咨询|weixin-微信咨询）',
  s_name STRING COMMENT '用户名称',
  seo_source STRING COMMENT '搜索来源',
  seo_keywords STRING COMMENT '关键字',
  ip STRING COMMENT 'IP地址',
  referrer STRING COMMENT '上级来源页面',
  from_url STRING COMMENT '会话来源页面',
  landing_page_url STRING COMMENT '访客着陆页面',
  url_title STRING COMMENT '咨询页面title',
  to_peer STRING COMMENT '所属技能组',
  manual_time STRING COMMENT '人工开始时间',
  begin_time STRING COMMENT '坐席领取时间 ',
  reply_msg_count int COMMENT '客服回复消息数',
  total_msg_count int COMMENT '消息总数',
  msg_count int COMMENT '客户发送消息数',
  comment STRING COMMENT '备注',
  finish_reason STRING COMMENT '结束类型',
  finish_user STRING COMMENT '结束坐席',
  end_time STRING COMMENT '会话结束时间',
  platform_description STRING COMMENT '客户平台信息',
  browser_name STRING COMMENT '浏览器名称',
  os_info STRING COMMENT '系统名称',
  area STRING COMMENT '区域',
  country STRING COMMENT '所在国家',
  province STRING COMMENT '省',
  city STRING COMMENT '城市',
  creator int COMMENT '创建人',
  name STRING COMMENT '客户姓名',
  idcard STRING COMMENT '身份证号',
  phone STRING COMMENT '手机号',
  itcast_school_id int COMMENT '校区Id',
  itcast_school STRING COMMENT '校区',
  itcast_subject_id int COMMENT '学科Id',
  itcast_subject STRING COMMENT '学科',
  wechat STRING COMMENT '微信',
  qq STRING COMMENT 'qq号',
  email STRING COMMENT '邮箱',
  gender STRING COMMENT '性别',
  level STRING COMMENT '客户级别',
  origin_type STRING COMMENT '数据来源渠道',
  information_way STRING COMMENT '资讯方式',
  working_years STRING COMMENT '开始工作时间',
  technical_directions STRING COMMENT '技术方向',
  customer_state STRING COMMENT '当前客户状态',
  valid STRING COMMENT '该线索是否是网资有效线索',
  anticipat_signup_date STRING COMMENT '预计报名时间',
  clue_state STRING COMMENT '线索状态',
  scrm_department_id int COMMENT 'SCRM内部部门id',
  superior_url STRING COMMENT '诸葛获取上级页面URL',
  superior_source STRING COMMENT '诸葛获取上级页面URL标题',
  landing_url STRING COMMENT '诸葛获取着陆页面URL',
  landing_source STRING COMMENT '诸葛获取着陆页面URL来源',
  info_url STRING COMMENT '诸葛获取留咨页URL',
  info_source STRING COMMENT '诸葛获取留咨页URL标题',
  origin_channel STRING COMMENT '投放渠道',
  course_id int COMMENT '课程编号',
  course_name STRING COMMENT '课程名称',
  zhuge_session_id STRING COMMENT 'zhuge会话id',
  is_repeat int COMMENT '是否重复线索(手机号维度) 0:正常 1：重复',
  tenant int COMMENT '租户id',
  activity_id STRING COMMENT '活动id',
  activity_name STRING COMMENT '活动名称',
  follow_type int COMMENT '分配类型，0-自动分配，1-手动分配，2-自动转移，3-手动单个转移，4-手动批量转移，5-公海领取',
  shunt_mode_id int COMMENT '匹配到的技能组id',
  shunt_employee_group_id int COMMENT '所属分流员工组',
  clue_state_stat STRING COMMENT '线索状态,1为新客户新线索,0为老客户新线索,-1为无效数据',

  customer_appeal_id int COMMENT '申诉id',
  employee_id int COMMENT '申诉人',
  employee_name string COMMENT '申诉人姓名',
  employee_department_id int COMMENT '申诉人部门',
  employee_tdepart_id int COMMENT '申诉人所属部门',
  appeal_status int COMMENT '申诉状态，0:待稽核 1:无效 2：有效',
  audit_id int COMMENT '稽核人id',
  audit_name string COMMENT '稽核人姓名',
  audit_department_id int COMMENT '稽核人所在部门',
  audit_department_name string COMMENT '稽核人部门名称',
  audit_date_time string  COMMENT '稽核时间',

  end_date string comment '有效时间'
)PARTITIONED BY(start_date string)
clustered by(customer_relationship_id) sorted by(customer_relationship_id) into 10 buckets
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');








-- edu_dwb层导入数据
-- 设置动态分区参数
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

-- relationship_2 维度转换
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=150000;



insert into edu_dwb.customer_clue_dwb partition(start_date)
SELECT
  cc.id,
  cc.create_date_time,
  cc.update_date_time,
  cc.deleted,
  cc.customer_id,
  cc.customer_relationship_id,
  cc.session_id,
  cc.sid,
  cc.status,
  cc.users,
  cc.create_time,
  cc.platform,
  cc.s_name,
  cc.seo_source,
  cc.seo_keywords,
  cc.ip,
  cc.referrer,
  cc.from_url,
  cc.landing_page_url,
  cc.url_title,
  cc.to_peer,
  cc.manual_time,
  cc.begin_time,
  cc.reply_msg_count,
  cc.total_msg_count,
  cc.msg_count,
  cc.comment,
  cc.finish_reason,
  cc.finish_user,
  cc.end_time,
  cc.platform_description,
  cc.browser_name,
  cc.os_info,
  cc.area,
  cc.country,
  cc.province,
  cc.city,
  cc.creator,
  cc.name,
  cc.idcard,
  cc.phone,
  cc.itcast_school_id,
  cc.itcast_school,
  cc.itcast_subject_id,
  cc.itcast_subject,
  cc.wechat,
  cc.qq,
  cc.email,
  cc.gender,
  cc.level,
  cc.origin_type,
  cc.information_way,
  cc.working_years,
  cc.technical_directions,
  cc.customer_state,
  cc.valid,
  cc.anticipat_signup_date,
  cc.clue_state,
  cc.scrm_department_id,
  cc.superior_url,
  cc.superior_source,
  cc.landing_url,
  cc.landing_source,
  cc.info_url,
  cc.info_source,
  cc.origin_channel,
  cc.course_id,
  cc.course_name,
  cc.zhuge_session_id,
  cc.is_repeat,
  cc.tenant,
  cc.activity_id,
  cc.activity_name,
  cc.follow_type,
  cc.shunt_mode_id,
  cc.shunt_employee_group_id,
  cc.clue_state_stat,
  ca.id as customer_appeal_id,
  ca.employee_id,
  ca.employee_name,
  ca.employee_department_id,
  ca.employee_tdepart_id,
  ca.appeal_status,
  ca.audit_id,
  ca.audit_name,
  ca.audit_department_id,
  ca.audit_department_name,
  ca.audit_date_time,
  cc.end_date,
  cc.start_date
from edu_dwd.fact_customer_clue cc
left join edu_dwd.fact_customer_appeal ca
on cc.customer_relationship_id=ca.customer_relationship_first_id
where ca.appeal_status !=1 or ca.appeal_status is null;  -- 筛选掉被申诉无效的数据













-- DM层建表
create table edu_dm.zx_dm(
    -- 计算指标
    customer_nums int COMMENT '意向客户总和',  -- 客户意向表
    clue_nums int COMMENT '有效线索总和',
    -- 按时间统计
    time_type string comment '时间分组',
    yearinfo string comment '年',
    monthinfo string comment '月',
    dayinfo string comment '日',
    hourinfo string comment '小时',
    --数据来源
    origin_type_state STRING COMMENT '数据来源:0.线下；1.线上',  -- 客户意向表
    --线索状态
    clue_state_stat STRING COMMENT '新老客户：0.老客户；1.新客户',  -- 客户线索表
    --其他维度
    group_type string comment '其他',
    area STRING COMMENT '所在区域',  -- 客户静态信息表
    itcast_subject_id int COMMENT '学科Id',  -- 学科表
    subject_name STRING COMMENT '学科名称',
    itcast_school_id int COMMENT '校区Id',  -- 校区表
    school_name string COMMENT '校区名称',
    tdepart_id int comment '直属部门id',  -- 员工表
    tdepart_name string COMMENT '部门名称')  -- 部门表
partitioned BY(dt STRING)
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compress'='SNAPPY');