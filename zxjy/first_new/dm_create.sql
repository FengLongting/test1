create database edu_dm;

drop table edu_dm.zx_dm;
create table edu_dm.zx_dm(
    -- 计算指标
    customer_nums int COMMENT '意向客户总和',  -- 客户意向表
    clue_nums int COMMENT '有效线索总和',
    -- 按时间统计
    time_type string comment '时间分组',
    yearinfo string comment '年',
    monthinfo string comment '月',
--     dayinfo string comment '日',
--     hourinfo string comment '小时',
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