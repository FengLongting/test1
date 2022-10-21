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
left join edu_dwd.fact_customer_relationship ll on cc.id = ll.customer_id
left join edu_dwd.fact_customer_appeal ca
on ll.id = ca.customer_relationship_first_id
where ca.appeal_status <> 1 or ca.appeal_status is null;  -- 筛选掉被申诉无效的数据

