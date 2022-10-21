--edu_dwd层
-- 拉链表
drop table edu_dwd.fact_customer_relationship;
CREATE TABLE IF NOT EXISTS edu_dwd.fact_customer_relationship (
  id int COMMENT '客户关系id',
  create_date_time STRING COMMENT '创建时间',
  update_date_time STRING COMMENT '最后更新时间',
  deleted string COMMENT '是否被删除（禁用）',
  customer_id int COMMENT '所属客户id',
  first_id int COMMENT '第一条客户关系id',
  belonger int COMMENT '归属人',
  belonger_name STRING COMMENT '归属人姓名',
  initial_belonger int COMMENT '初始归属人',
  distribution_handler int COMMENT '分配处理人',
  business_scrm_department_id int COMMENT '归属部门',
  last_visit_time STRING COMMENT '最后回访时间',
  next_visit_time STRING COMMENT '下次回访时间',
  origin_type STRING COMMENT '数据来源',
  itcast_school_id int COMMENT '校区Id',
  itcast_subject_id int COMMENT '学科Id',
  intention_study_type STRING COMMENT '意向学习方式',
  anticipat_signup_date STRING COMMENT '预计报名时间',
  level STRING COMMENT '客户级别',
  creator int COMMENT '创建人',
  current_creator int COMMENT '当前创建人：初始==创建人，当在公海拉回时为 拉回人',
  creator_name STRING COMMENT '创建者姓名',
  origin_channel STRING COMMENT '来源渠道',
  comment STRING COMMENT '备注',
  first_customer_clue_id int COMMENT '第一条线索id',
  last_customer_clue_id int COMMENT '最后一条线索id',
  process_state STRING COMMENT '处理状态',
  process_time STRING COMMENT '处理状态变动时间',
  payment_state STRING COMMENT '支付状态',
  payment_time STRING COMMENT '支付状态变动时间',
  signup_state STRING COMMENT '报名状态',
  signup_time STRING COMMENT '报名时间',
  notice_state STRING COMMENT '通知状态',
  notice_time STRING COMMENT '通知状态变动时间',
  lock_state STRING COMMENT '锁定状态',
  lock_time STRING COMMENT '锁定状态修改时间',
  itcast_clazz_id int COMMENT '所属ems班级id',
  itcast_clazz_time STRING COMMENT '报班时间',
  payment_url STRING COMMENT '付款链接',
  payment_url_time STRING COMMENT '支付链接生成时间',
  ems_student_id int COMMENT 'ems的学生id',
  delete_reason STRING COMMENT '删除原因',
  deleter int COMMENT '删除人',
  deleter_name STRING COMMENT '删除人姓名',
  delete_time STRING COMMENT '删除时间',
  course_id int COMMENT '课程ID',
  course_name STRING COMMENT '课程名称',
  delete_comment STRING COMMENT '删除原因说明',
  close_state STRING COMMENT '关闭装填',
  close_time STRING COMMENT '关闭状态变动时间',
  appeal_id int COMMENT '申诉id',
  tenant int COMMENT '租户',
  total_fee DECIMAL COMMENT '报名费总金额',
  belonged int COMMENT '小周期归属人',
  belonged_time STRING COMMENT '归属时间',
  belonger_time STRING COMMENT '归属时间',
  transfer int COMMENT '转移人',
  transfer_time STRING COMMENT '转移时间',
  follow_type int COMMENT '分配类型，0-自动分配，1-手动分配，2-自动转移，3-手动单个转移，4-手动批量转移，5-公海领取',
  transfer_bxg_oa_account STRING COMMENT '转移到博学谷归属人OA账号',
  transfer_bxg_belonger_name STRING COMMENT '转移到博学谷归属人OA姓名',
  origin_type_state STRING COMMENT '数据来源,线上1,线下0',
--   yearinfo string comment '年',
--   monthinfo string comment '月',
--   hourinfo string comment '小时',
--   day_info string comment '天',
  end_date string comment '有效时间'
)comment '客户关系表'
PARTITIONED BY(yearinfo STRING,monthinfo STRING)
CLUSTERED BY (id) sorted by (id) into 10 buckets
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');


-- 客户线索表
CREATE TABLE IF NOT EXISTS edu_dwd.fact_customer_clue (
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
  end_date string comment '有效时间'
)comment '客户关系表'
PARTITIONED BY(start_date string)
clustered by(customer_relationship_id) sorted by(customer_relationship_id) into 10 buckets
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 客户表:
CREATE TABLE IF NOT EXISTS edu_dwd.dim_customer (
  id int COMMENT 'key id',
  customer_relationship_id int COMMENT '当前意向id',
  create_date_time STRING COMMENT '创建时间',
  update_date_time STRING COMMENT '最后更新时间',
  deleted int  COMMENT '是否被删除（禁用）',
  name STRING COMMENT '姓名',
  idcard STRING  COMMENT '身份证号',
  birth_year int COMMENT '出生年份',
  gender STRING COMMENT '性别',
  phone STRING COMMENT '手机号',
  wechat STRING COMMENT '微信',
  qq STRING COMMENT 'qq号',
  email STRING COMMENT '邮箱',
  area STRING COMMENT '所在区域',
  leave_school_date date COMMENT '离校时间',
  graduation_date date COMMENT '毕业时间',
  bxg_student_id STRING COMMENT '博学谷学员ID，可能未关联到，不存在',
  creator int COMMENT '创建人ID',
  origin_type STRING COMMENT '数据来源',
  origin_channel STRING COMMENT '来源渠道',
  tenant int,
  md_id int COMMENT '中台id'
)comment '客户表'
PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 学科表
CREATE TABLE IF NOT EXISTS edu_dwd.dim_itcast_subject (
  id int COMMENT '自增主键',
  create_date_time timestamp COMMENT '创建时间',
  update_date_time timestamp COMMENT '最后更新时间',
  deleted STRING COMMENT '是否被删除（禁用）',
  name STRING COMMENT '学科名称',
  code STRING COMMENT '学科编码',
  tenant int COMMENT '租户'
)comment '学科字典表'
PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');

-- itcast_school校区表
CREATE TABLE IF NOT EXISTS edu_dwd.dim_itcast_school(
  id int comment '主键自增',
  create_date_time string COMMENT '创建时间',
  update_date_time string COMMENT '最后更新时间',
  deleted string COMMENT '是否被删除（禁用）',
  name string COMMENT '校区名称',
  code string COMMENT '校区标识',
  tenant int COMMENT '租户'
) comment '校区表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compress'='SNAPPY');

--scrm_department 部门表
CREATE TABLE IF NOT EXISTS edu_dwd.dim_scrm_department (
  id int COMMENT '部门id',
  name string COMMENT '部门名称',
  parent_id int COMMENT '父部门id',
  create_date_time string COMMENT '创建时间',
  update_date_time string COMMENT '更新时间',
  deleted string COMMENT '删除标志',
  id_path string COMMENT '编码全路径',
  tdepart_code int COMMENT '直属部门',
  creator string COMMENT '创建者',
  depart_level int COMMENT '部门层级',
  depart_sign int COMMENT '部门标志，暂时默认1',
  depart_line int COMMENT '业务线，存储业务线编码',
  depart_sort int COMMENT '排序字段',
  disable_flag int COMMENT '禁用标志',
  tenant int COMMENT '租户'
) comment '部门表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compress'='SNAPPY');

--员工表employee
create TABLE IF NOT EXISTS edu_dwd.dim_employee
(
    id                  int       COMMENT '员工id',
    email               string    comment '公司邮箱，OA登录账号',
    real_name           string    comment '员工的真实姓名',
    phone               string    comment '手机号，目前还没有使用；隐私问题OA接口没有提供这个属性，',
    department_id       string    comment 'OA中的部门编号，有负值',
    department_name     string    comment 'OA中的部门名',
    remote_login        tinyint   comment '员工是否可以远程登录',
    job_number          string    comment '员工工号',
    cross_school        tinyint   comment '是否有跨校区权限',
    last_login_date     string  comment '最后登录日期',
    creator             int       comment '创建人',
    create_date_time    string  comment '创建时间',
    update_date_time    string comment '最后更新时间',
    deleted             tinyint   comment '是否被删除（禁用）',
    scrm_department_id  tinyint   comment 'SCRM内部部门id',
    leave_office        tinyint   comment '离职状态',
    leave_office_time   string  comment '离职时间',
    reinstated_time     string  comment '复职时间',
    superior_leaders_id int       comment '上级领导ID',
    tdepart_id          int       comment '直属部门',
    tenant              int COMMENT '租户',
    ems_user_name       string
)comment '员工信息表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compress'='SNAPPY');

--customer_appeal 线索申诉表
CREATE TABLE IF NOT EXISTS edu_dwd.fact_customer_appeal (
  id int COMMENT 'customer_appeal_id',
  customer_relationship_first_id int COMMENT '第一条客户关系id',
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
  create_date_time string  COMMENT '创建时间（申诉时间）',
  update_date_time string  COMMENT '更新时间',
  deleted tinyint  COMMENT '删除标志位',
  tenant string COMMENT '租户id'
) comment '线索申诉表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compress'='SNAPPY',    'orc.bloom.filter.columns'='appeal_status,customer_relationship_first_id');


