SHOW DATABASES;
USE zxjy;
show tables;


SELECT COUNT(*)
FROM zxjy_result;
-- # 第一题 全部总数
SELECT customer_nums
FROM zxjy_result
WHERE group_type = 'all'
  AND time_type is null
order by customer_nums desc limit 1
;

SELECT sum(customer_nums),yearinfo
FROM zxjy_result
WHERE group_type = 'all'
and time_type = 'year' and clue_state_stat = 1
GROUP BY yearinfo
;

SELECT sum(customer_nums),monthinfo,yearinfo
FROM zxjy_result
WHERE  time_type = 'month' AND group_type = 'all' and clue_state_stat = 1
GROUP BY monthinfo,yearinfo
;

-- # 主题二：
SELECT sum(customer_nums),yearinfo,area
FROM zxjy_result
WHERE  time_type = 'year' AND group_type = 'c.area' and area is not NULL
group by yearinfo, area
;

-- # 月某地
SELECT sum(customer_nums),yearinfo,monthinfo,area
FROM zxjy_result
WHERE  time_type = 'month' AND group_type = 'c.area' and clue_state_stat = 1 and area is not NULL
GROUP BY yearinfo,monthinfo,area
;


-- #3主题三需求三: 统计指定时间段内，新增的意向客户中，意向学科人数排行榜。学科名称要关联查询出来
-- # 年
SELECT sum(customer_nums),yearinfo,itcast_subject_id,subject_name
FROM zxjy_result
WHERE  time_type = 'year' AND group_type = 'subject' and itcast_subject_id <> -1
group by yearinfo, itcast_subject_id,subject_name
ORDER BY yearinfo,sum(customer_nums) desc;
;
-- #月
SELECT sum(customer_nums),yearinfo,monthinfo,itcast_subject_id,subject_name
FROM zxjy_result
WHERE  time_type = 'month' AND group_type = 'subject' and itcast_subject_id <> -1
group by yearinfo, monthinfo,itcast_subject_id,subject_name
ORDER BY yearinfo,monthinfo,sum(customer_nums) desc;
;


-- # 主题四 需求四: 统计指定时间段内，新增的意向客户中，意向校区人数排行榜。
SELECT sum(customer_nums),yearinfo,itcast_school_id,school_name
FROM zxjy_result
WHERE  time_type = 'year' AND group_type = 'school' and clue_state_stat = 1
group by yearinfo, itcast_school_id,school_name
ORDER BY yearinfo,sum(customer_nums) desc;
;

SELECT sum(customer_nums),yearinfo,monthinfo,itcast_school_id,school_name
FROM zxjy_result
WHERE  time_type = 'month' AND group_type = 'school' and clue_state_stat = 1
group by yearinfo, monthinfo,itcast_school_id,school_name
ORDER BY yearinfo,monthinfo,sum(customer_nums) desc;
;




-- # 需求五: 统计指定时间段内，新增的意向客户中，不同来源渠道的意向客户占比。
-- #年
use zxjy;
# drop table view_year_origin;
create table view_year_origin as
SELECT sum(customer_nums) as second,yearinfo,origin_type_state
FROM zxjy_result
WHERE  time_type = 'year'AND group_type = 'all' and clue_state_stat = 1
group by yearinfo, origin_type_state
ORDER BY yearinfo,sum(customer_nums) desc;
;
create table view_year_origin1 as
SELECT sum(customer_nums) as first,yearinfo
FROM zxjy_result
WHERE  time_type = 'year' AND group_type = 'all' and clue_state_stat = 1
group by yearinfo
ORDER BY yearinfo,sum(customer_nums) desc;
;

-- # result
seLECT view_year_origin.second/view_year_origin1.first as percent,view_year_origin.yearinfo,view_year_origin.origin_type_state
    from view_year_origin1 left join view_year_origin on view_year_origin1.yearinfo = view_year_origin.yearinfo;



-- # 月
# drop table view_month_origin1;
create table view_month_origin as
SELECT sum(customer_nums) as second,yearinfo,monthinfo,origin_type_state
FROM zxjy_result
WHERE  time_type = 'month' AND group_type = 'school' and clue_state_stat = 1
group by yearinfo, monthinfo,origin_type_state
;

create table view_month_origin1 as
SELECT sum(customer_nums) as first,yearinfo,monthinfo
FROM zxjy_result
WHERE  time_type = 'month' AND group_type = 'school' and clue_state_stat = 1
group by yearinfo, monthinfo
-- # ORDER BY yearinfo,monthinfo,sum(customer_nums) desc;
;
-- # seLECT* from view_month_origin1;
seLECT view_month_origin.second/view_month_origin1.first as percent,view_month_origin.yearinfo,view_month_origin.monthinfo,view_month_origin.origin_type_state
    from view_month_origin left join view_month_origin1 on view_month_origin1.yearinfo = view_month_origin.yearinfo and view_month_origin.monthinfo = view_month_origin1.monthinfo
order by view_month_origin.yearinfo,view_month_origin.monthinfo;













# 需求六: 统计指定时间段内，新增的意向客户中，各咨询中心产生的意向客户数占比情况。
# SELECT sum(customer_nums),yearinfo,tdepart_id,tdepart_name
# FROM zxjy_result
# WHERE  time_type = 'year' AND group_type = 'tdepart' and clue_state_stat = 1 and tdepart_id is not NULL
# group by yearinfo, tdepart_id,tdepart_name
# ORDER BY yearinfo,sum(customer_nums) desc;
# ;

CREATE TABLE tmp_table as
SELECT sum(customer_nums) as all_num,yearinfo
FROM zxjy_result
WHERE  time_type = 'year' AND group_type = 'tdepart' and tdepart_id is not NULL and clue_state_stat = 1
group by yearinfo;

# drop table tmp_table1;
CREATE TABLE tmp_table1 as
SELECT sum(customer_nums) as p,yearinfo,tdepart_id,tdepart_name
FROM zxjy_result
WHERE  time_type = 'year' AND group_type = 'tdepart' and clue_state_stat = 1 and tdepart_id is not NULL
group by yearinfo, tdepart_id,tdepart_name
ORDER BY yearinfo,sum(customer_nums) desc;
;
#  该咨询中心占比
SELECT tmp_table1.p/tmp_table.all_num,tmp_table1.yearinfo,tdepart_id,tdepart_name
from tmp_table1 left join tmp_table on tmp_table1.yearinfo = tmp_table.yearinfo;




# SELECT sum(customer_nums),yearinfo,monthinfo,tdepart_id,tdepart_name
# FROM zxjy_result
# WHERE  time_type = 'month' AND group_type = 'tdepart' and clue_state_stat = 1
# group by yearinfo, monthinfo,tdepart_id,tdepart_name
# ORDER BY yearinfo,monthinfo,sum(customer_nums) desc;
# ;

CREATE table tmp_table_month as
SELECT sum(customer_nums) as all_num,yearinfo,monthinfo
FROM zxjy_result
WHERE  time_type = 'month' AND group_type = 'tdepart' and tdepart_id is not NULL and clue_state_stat = 1
group by yearinfo, monthinfo;

# drop view tmp_table_month;
CREATE table tmp_table1_month as
SELECT sum(customer_nums) as p,yearinfo,monthinfo,tdepart_id,tdepart_name
FROM zxjy_result
WHERE  time_type = 'month' AND group_type = 'tdepart' and clue_state_stat = 1 and tdepart_id is not NULL
group by yearinfo, monthinfo,tdepart_id,tdepart_name
# ORDER BY yearinfo,monthinfo,sum(customer_nums) desc;
;

# 每月都没有交集
# seLECT * FROM tmp_table1_month;
SELECT if(tmp_table1_month.p/tmp_table_month.all_num is null,0,tmp_table1_month.p/tmp_table_month.all_num) as percent,tmp_table1_month.yearinfo,tmp_table1_month.monthinfo,tdepart_id,tdepart_name
from tmp_table1_month left join tmp_table_month ON tmp_table1_month.yearinfo = tmp_table_month.yearinfo and tmp_table1_month.monthinfo = tmp_table_month.monthinfo;









# 需求七: 统计期内，访客咨询产生的有效线索的占比。有效线索量 / 咨询量，有效线索指的是拿到电话且电话有效。
# select * from zxjy_result;
# 看全部_______有效线索量 / 咨询量
SELECT sum(clue_nums)/sum(customer_nums) as all_numo
FROM zxjy_result
WHERE  time_type is null AND group_type = 'all';
# DROP VIEW XIANZUO_YEAR;
#看年_______有效线索量 / 咨询量
create table XIANZUO_YEAR AS
SELECT sum(clue_nums) AS ALL_MUM ,yearinfo
FROM zxjy_result
WHERE  time_type ='all' AND group_type = 'all' and yearinfo is not NULL
GROUP BY yearinfo;

#看年_______总咨询量
create table ZIXUN_YEAR AS
SELECT sum(customer_nums) AS ALL_MUN,yearinfo
FROM zxjy_result
WHERE  time_type ='year' AND group_type = 'all'
group by yearinfo;

SELECT A.ALL_MUM/Y.ALL_MUN as percent,A.yearinfo
from XIANZUO_YEAR A left join ZIXUN_YEAR Y ON A.yearinfo = Y.yearinfo;




# select * from zxjy_result;
#看月_______有效线索量 / 咨询量
create table XIANZUO_month AS
SELECT sum(clue_nums) AS ALL_MUM ,yearinfo,monthinfo
FROM zxjy_result
WHERE  time_type ='all' AND group_type = 'all' and yearinfo is not NULL
GROUP BY yearinfo,monthinfo;

#看月_______总咨询量
# DROP VIEW ZIXUN_month;
create table ZIXUN_month AS
SELECT sum(customer_nums) AS ALL_MUN,yearinfo,monthinfo
FROM zxjy_result
WHERE  time_type ='month' AND group_type = 'all'
group by yearinfo,monthinfo;

SELECT A.ALL_MUM/Y.ALL_MUN as percent,A.yearinfo,A.monthinfo
from XIANZUO_month A left join ZIXUN_month Y ON A.yearinfo = Y.yearinfo and A.monthinfo = Y.monthinfo;


# 需求九:统计期内，新增的咨询客户中，有效线索的数量。
# select * from zxjy_result where  time_type = 'year' and group_type = 'all'
SELECT A.ALL_MUM,A.yearinfo
from XIANZUO_YEAR A left join ZIXUN_YEAR Y ON A.yearinfo = Y.yearinfo;

SELECT A.ALL_MUM,Y.yearinfo,Y.monthinfo
from ZIXUN_month Y left join XIANZUO_month A on Y.yearinfo = A.yearinfo and Y.monthinfo = A.monthinfo
order by Y.yearinfo,Y.monthinfo ;





