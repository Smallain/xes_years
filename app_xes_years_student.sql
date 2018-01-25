
------------------------------------------学员范围表------------------------------------------
--学员全量表，包含两部分数据
--1.报班表缴费状态为1【已缴费】
--2.开课后退费的学员
--取学员id，分校id和所报班级id



---学生范围信息（班级和分校和学生id）
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_info AS
SELECT DISTINCT t.stu_id,
    t.city_id,
    t.city_name,
    t.cla_id
FROM ( SELECT stu_id,
            city_id,
            city_name,
            cur_cla_id AS cla_id
        FROM odata_dev.dw_regist
        WHERE pay_flag = '1' and SUBSTR(reg_time,1,10)<='2018-01-08'
        UNION ALL
        SELECT stu_id,
            city_id,
            city_name,
            rc_cla_id AS cla_id
        FROM odata_dev.DW_STU_RET_INFO
        WHERE RET_CLA_TYPE = '2' and SUBSTR(refund_time,1,10)<='2018-01-08') t ;


--学员注册时间和学习年限和分校排名（根据学生注册时间）
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_city_rank_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_city_rank_info AS        
select 
city_id
,city_name
,stu_id 
,stu_name
,stu_register_time
,stu_city_rank 
,stu_study_years
  from (        
        select city_id
        ,city_name
        ,stu_id 
        ,regexp_replace(stu_real_name,'[0-9,a-z,A-Z,（,）,(,)]','')     AS stu_name
        ,create_time AS stu_register_time
        ,row_number() over(partition BY city_id,city_name ORDER BY create_time ASC) AS stu_city_rank 
        ,CAST((2018-SUBSTR(create_time,1,4)) AS INT) AS stu_study_years
        from odata_dev.dw_student  where is_del ='0'
        and SUBSTR(create_time,1,10)<='2018-01-08'
 ) t ; 

        
--通过注册表  开始注册的课次数，找到学生开始上课的所有班次
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_regist_cuc_start_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_regist_cuc_start_info AS       
select 
regist_info.city_id
,regist_info.stu_id
,regist_info.cur_cla_id
,(class_info.total_cuc_cnt-regist_info.reg_cuc_cnt+1) as cuc_start_no --班级总课次-报班课次+1=开始课次
,class_info.venue_id
,class_info.venue_name
,class_info.subj_id
,class_info.subj_name
,class_info.class_type
from (
        select city_id
        ,stu_id
        ,cur_cla_id
        ,reg_cuc_cnt
        from odata_dev.dw_regist 
        where pay_flag = '1' and SUBSTR(reg_time,1,10)<='2018-01-08' 
) regist_info join
(select city_id
        ,cla_id
        ,total_cuc_cnt
        ,venue_id
        ,venue_name
        ,subj_id
        ,subj_name
        ,class_type
        from odata_dev.dw_class
        where  is_del='0' and is_lvl_can='0' and IS_CLA_CLO='0' 
        --长期班+短期班
         and term_id IN ('1','2','3','4','5')
        --在线、双师、面授
         and class_type in ('1','2','4')
        ) class_info on regist_info.city_id=class_info.city_id 
                                                             and regist_info.cur_cla_id = class_info.cla_id;


--通过退班表  开始退课的课次数，找到学生退课开始的所有班次
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_return_cuc_start_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_return_cuc_start_info AS       
select 
return_info.city_id
,return_info.stu_id
,return_info.rc_cla_id
,(class_info.total_cuc_cnt-return_info.reg_cuc_cnt+1) as cuc_start_no --班级总课次-报班课次+1=退课开始课次
,class_info.venue_id
,class_info.venue_name
,class_info.subj_id
,class_info.subj_name
,class_info.class_type
from (
        select city_id,stu_id
        ,rc_cla_id
        ,reg_cuc_cnt
         from odata_dev.dw_stu_ret_info 
         WHERE RET_CLA_TYPE = '2' and SUBSTR(refund_time,1,10)<='2018-01-08'  
) return_info join
(select city_id
        ,cla_id
        ,total_cuc_cnt 
        ,venue_id
        ,venue_name
        ,subj_id
        ,subj_name
        ,class_type
        from odata_dev.dw_class
        where  is_del='0' and is_lvl_can='0' and IS_CLA_CLO='0' 
        --长期班+短期班
         and term_id IN ('1','2','3','4','5')
        --在线、双师、面授
         and class_type in ('1','2','4')
        ) class_info on return_info.city_id=class_info.city_id 
                                                             and return_info.rc_cla_id = class_info.cla_id;








--所有注册学员上过的课次合计详细信息
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_regist_cuc_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_regist_cuc_info AS 
select 
reg_start.city_id
,reg_start.stu_id
,reg_start.cur_cla_id
,concat(ddc.cuc_date,' ',ddc.cuc_s_time)  as class_time
,reg_start.venue_id
,reg_start.venue_name
,ddc.tea_id
,reg_start.subj_id
,reg_start.subj_name
,reg_start.class_type
from odata_dev.app_xes_years_student_regist_cuc_start_info reg_start 
join odata_dev.dw_curriculum  ddc on reg_start.city_id = ddc.city_id and reg_start.cur_cla_id = ddc.cla_id
where reg_start.cuc_start_no<= ddc.cuc_no; --筛选出所有上过的课次



--所有退费学员上过的课次合计详细信息
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_return_cuc_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_return_cuc_info AS 
select 
ret_start.city_id
,ret_start.stu_id
,ret_start.rc_cla_id
,concat(ddc.cuc_date,' ',ddc.cuc_s_time)  as class_time
,ret_start.venue_id
,ret_start.venue_name
,ddc.tea_id
,ret_start.subj_id
,ret_start.subj_name
,ret_start.class_type
from odata_dev.app_xes_years_student_return_cuc_start_info ret_start 
join odata_dev.dw_curriculum  ddc on ret_start.city_id = ddc.city_id and ret_start.rc_cla_id = ddc.cla_id
where ret_start.cuc_start_no>= ddc.cuc_no; --筛选出所有上过的课次



--合并 注册和退费学员的课程合计详细信息
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_cuc_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_cuc_info AS 
select 
city_id
,stu_id
,cur_cla_id as cla_id
,class_time
,venue_id
,venue_name
,tea_id
,subj_id
,subj_name
,class_type
from odata_dev.app_xes_years_student_regist_cuc_info
union all
select
city_id
,stu_id
,rc_cla_id as cla_id
,class_time
,venue_id
,venue_name
,tea_id
,subj_id
,subj_name
,class_type
from odata_dev.app_xes_years_student_return_cuc_info;







--第一次课信息
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_first_class_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_first_class_info AS 
select 
city_id
,stu_id
,class_time as stu_first_class_time
,venue_id as stu_first_class_venue_id
,venue_name as stu_first_class_venue_name
,tea_id as stu_first_class_teacher_id
,subj_id as stu_first_class_subject_id
,subj_name as stu_first_class_subject_name
from (
select
city_id
,stu_id
,cla_id
,class_time
,venue_id
,venue_name
,tea_id
,subj_id
,subj_name
,row_number() over(partition BY city_id,stu_id ORDER BY class_time ASC) AS rank_num
from odata_dev.app_xes_years_student_cuc_info) temp
where rank_num ='1';





--第一次在线课信息
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_first_online_class_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_first_online_class_info AS 
select 
city_id
,stu_id
,class_time as stu_first_class_time
,venue_id as stu_first_class_venue_id
,venue_name as stu_first_class_venue_name
,tea_id as stu_first_class_teacher_id
,subj_id as stu_first_class_subject_id
,subj_name as stu_first_class_subject_name
from (
select
city_id
,stu_id
,cla_id
,class_time
,venue_id
,venue_name
,tea_id
,subj_id
,subj_name
,row_number() over(partition BY city_id,stu_id ORDER BY class_time ASC) AS rank_num
from odata_dev.app_xes_years_student_cuc_info
where class_type = '1'
) temp
where rank_num ='1';



--学生统计信息
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_statistics_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_statistics_info AS 
select city_id
,stu_id
,count(*) as stu_curriculum_amounts
,count(*)*11 as stu_handout_numbers
,round(count(*)*2.6,2) as stu_study_time_amounts
from odata_dev.app_xes_years_student_cuc_info
group by city_id,stu_id;


--努力程度
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_strive_info;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_strive_info AS 
select 
statistics_info.city_id
,statistics_info.stu_id
,concat(cast(round((1-(statistics_info.stu_strive_rank/city_num.city_stu_num))*100,2) as string),'%') as stu_strive_ratio
from 
(select 
city_id
,stu_id
,row_number() over(partition by city_id order by stu_curriculum_amounts desc) as stu_strive_rank
from odata_dev.app_xes_years_student_statistics_info ) statistics_info
join 
(select city_id
,count(*) as city_stu_num
from odata_dev.dw_student  where is_del ='0' and SUBSTR(create_time,1,10)<='2018-01-08' group by city_id) city_num
on statistics_info.city_id = city_num.city_id;



------------------------------------------学员班级类型-----------------------------------------

--学员总量表
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_total;
CREATE TABLE odata_dev.app_xes_years_student_total as 
SELECT distinct
        stu_id            
        ,city_id           
        ,city_name      
        ,cla_id            
FROM (
        SELECT 
                stu_id
                ,city_id
                ,city_name
                ,cur_cla_id AS cla_id
        FROM odata_dev.dw_regist
        WHERE pay_flag = '1'           --净缴费
        UNION ALL
        SELECT 
                stu_id
                ,city_id
                ,city_name
                ,rc_cla_id AS cla_id
        FROM odata_dev.DW_STU_RET_INFO
        WHERE RET_CLA_TYPE = '2'   --开课后退班
        )temp;



DROP TABLE IF EXISTS odata_dev.app_xes_years_student_class_type;
CREATE TABLE odata_dev.app_xes_years_student_class_type AS
  SELECT
    city_id,
    stu_id,
    sum(class_type) AS class_type
  FROM (
  --学员总体班级类型
         SELECT
           city_id,
           stu_id,
           class_type
         FROM (
                SELECT
                  odaxyst.stu_id,
                  odaxyst.city_id,
                  odaxyst.cla_id,
                  oddc.class_type
                FROM odata_dev.app_xes_years_student_total odaxyst
                  JOIN odata_dev.dw_class oddc ON odaxyst.city_id = oddc.city_id AND odaxyst.cla_id = oddc.cla_id
                          AND oddc.is_del = '0' AND oddc.is_lvl_can = '0' AND oddc.IS_CLA_CLO = '0'
                          --长期班 + 短期班
                          AND oddc.term_id IN ('1', '2', '3', '4', '5')
                           ---在线、双师、面授
                           AND oddc.class_type IN ('1', '2', '4')
              ) temp
         GROUP BY city_id, stu_id, class_type) temp2
  GROUP BY city_id, stu_id;


--1在线  2双师 4:面授
DROP TABLE IF EXISTS odata_dev.app_xes_years_student_class_type_3;
CREATE TABLE odata_dev.app_xes_years_student_class_type_3 AS
select 
        city_id
        ,stu_id
        ,case when class_type = '4' then '1' --纯面授
        when class_type = '1' then '2'         --纯在线
        when class_type = '5' then '3'         --面授加在线
        when class_type = '2' then '4'         --纯双师
        when class_type = '6' then '5'         --面授加双师
        when class_type = '3' then '6'         --双师加在线
        when class_type = '7' then '7'         --面授加双师加在线
        end as class_type
from odata_dev.app_xes_years_student_class_type ;







-------------------------------------------------------------------结果---------------------------------------------------------------------------------------



DROP TABLE IF EXISTS odata_dev.app_xes_years_student_data;
CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_student_data AS
select 
concat(city_rank_info.city_id,city_rank_info.stu_id) as id
,city_rank_info.stu_id
,city_rank_info.stu_name
,city_rank_info.stu_register_time
,city_rank_info.city_id
,city_rank_info.city_name
,city_rank_info.stu_city_rank
,first_class_info.stu_first_class_time
,class_type3.class_type
,first_class_info.stu_first_class_venue_id
,first_class_info.stu_first_class_venue_name
,first_class_info.stu_first_class_teacher_id
,regexp_replace(dt.tea_real_name,'[0-9,a-z,A-Z,（,）,(,)]','')     AS stu_first_class_teacher_name
,first_class_info.stu_first_class_subject_id
,first_class_info.stu_first_class_subject_name
,city_rank_info.stu_study_years
,statistics_info.stu_curriculum_amounts
,statistics_info.stu_handout_numbers
,first_online_class_info.stu_first_class_time as stu_first_online_class_time
,statistics_info.stu_study_time_amounts
,strive_info.stu_strive_ratio
from odata_dev.app_xes_years_student_city_rank_info city_rank_info
join odata_dev.app_xes_years_student_first_class_info first_class_info on city_rank_info.city_id = first_class_info.city_id and city_rank_info.stu_id = first_class_info.stu_id
left join odata_dev.app_xes_years_student_first_online_class_info first_online_class_info on city_rank_info.city_id = first_online_class_info.city_id and city_rank_info.stu_id = first_online_class_info.stu_id
join odata_dev.app_xes_years_student_statistics_info  statistics_info on city_rank_info.city_id = statistics_info.city_id and city_rank_info.stu_id=statistics_info.stu_id
join odata_dev.app_xes_years_student_strive_info  strive_info on city_rank_info.city_id = strive_info.city_id and city_rank_info.stu_id = strive_info.stu_id
join odata_dev.app_xes_years_student_class_type_3 class_type3 on city_rank_info.city_id = class_type3.city_id and city_rank_info.stu_id = class_type3.stu_id
left join odata_dev.dw_teacher dt on first_class_info.city_id = dt.city_id and first_class_info.stu_first_class_teacher_id = dt.tea_id;






insert overwrite table otemp.APP_XES_YEARS_STUDENT_TEST
select * from odata_dev.app_xes_years_student_data;


select count(*) from odata_dev.app_xes_years_student_data ;

select count(*) from otemp.APP_XES_YEARS_STUDENT_TEST;

select * from otemp.APP_XES_YEARS_STUDENT_TEST where stu_id = '0000000049630b70014968edd7083ff7';


select * from otemp.APP_XES_YEARS_STUDENT_TEST limit 100;