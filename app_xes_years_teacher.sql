
---分校内排名(教师与辅导教师一起参加排序)排序方式是创建时间+员工编号，创建时间在前
DROP TABLE IF EXISTS odata_dev.app_xes_years_city_rank_info;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_city_rank_info AS
SELECT tea_id,
       city_id,
       row_number() over(partition BY city_id
                         ORDER BY create_time ASC,emp_no ASC) AS tea_city_rank
FROM
 (SELECT tea_id,
         city_id,
         substr(create_time,1,10) AS create_time,
         emp_no
  FROM odata_dev.dw_teacher
  WHERE tea_state = '0'
   AND substr(create_time,1,10)<='2018-01-08'
  UNION ALL SELECT tutor_id AS tea_id,
                   city_id,
                   substr(create_time,1,10) AS create_time,
                   emp_no
  FROM odata_dev.dw_tutor
  WHERE is_freeze = '0'
   AND substr(create_time,1,10)<='2018-01-08') t;



--教师表筛选出符合标准的人员与分校排名表进行关联

DROP TABLE IF EXISTS odata_dev.app_xes_years_tea_city_rank_info;

CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_tea_city_rank_info AS
SELECT dt.tea_id ,
       dt.city_id ,
       city_rank_info.tea_city_rank
FROM odata_dev.dw_teacher dt
JOIN odata_dev.app_xes_years_city_rank_info city_rank_info ON dt.city_id = city_rank_info.city_id
AND dt.tea_id = city_rank_info.tea_id
WHERE dt.tea_state = '0'
 AND substr(dt.create_time,1,10)<='2018-01-08';

--辅导老师表筛选出符合标准的人员与分校排名表进行关联
DROP TABLE IF EXISTS odata_dev.app_xes_years_tut_city_rank_info;

CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_tut_city_rank_info AS
SELECT dtu.tutor_id AS tea_id ,
       dtu.city_id ,
       city_rank_info.tea_city_rank
FROM odata_dev.dw_tutor dtu
JOIN odata_dev.app_xes_years_city_rank_info city_rank_info ON dtu.city_id = city_rank_info.city_id
AND dtu.tutor_id = city_rank_info.tea_id
WHERE dtu.is_freeze = '0'
 AND substr(dtu.create_time,1,10)<='2018-01-08';











 --关联注册表获取班级与教师关系表

DROP TABLE IF EXISTS odata_dev.app_xes_years_tea_class_info;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_tea_class_info AS
SELECT city_id ,
       cla_id ,
       tea_id ,
       tutor_id ,
       venue_id ,
       venue_name ,
       lvl_id ,
       lvl_name
FROM odata_dev.dw_class
WHERE is_del='0'
 AND is_lvl_can='0'
 AND is_cla_clo='0' --长期班+短期班

 AND term_id IN ('1',
                 '2',
                 '3',
                 '4',
                 '5') --在线、双师、面授

 AND class_type IN ('1',
                    '2',
                    '4')
 AND cla_s_date<='2018-01-08' ;

 --主讲老师关联班级

DROP TABLE IF EXISTS odata_dev.app_xes_years_tea_class_info_join_tea;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_tea_class_info_join_tea AS
SELECT tea_city_rank_info.city_id ,
       tea_city_rank_info.tea_id ,
       tea_city_rank_info.tea_city_rank ,
       tea_class_info.cla_id ,
       tea_class_info.venue_id ,
       tea_class_info.venue_name ,
       tea_class_info.lvl_id ,
       tea_class_info.lvl_name
FROM odata_dev.app_xes_years_tea_city_rank_info tea_city_rank_info
JOIN odata_dev.app_xes_years_tea_class_info tea_class_info ON tea_city_rank_info.city_id = tea_class_info.city_id
AND tea_city_rank_info.tea_id = tea_class_info.tea_id;

 --辅导老师关联班级

DROP TABLE IF EXISTS odata_dev.app_xes_years_tea_class_info_join_tut;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_tea_class_info_join_tut AS
SELECT tut_city_rank_info.city_id ,
       tut_city_rank_info.tea_id ,
       tut_city_rank_info.tea_city_rank ,
       tea_class_info.cla_id ,
       tea_class_info.venue_id ,
       tea_class_info.venue_name ,
       tea_class_info.lvl_id ,
       tea_class_info.lvl_name
FROM odata_dev.app_xes_years_tut_city_rank_info tut_city_rank_info
JOIN odata_dev.app_xes_years_tea_class_info tea_class_info ON tut_city_rank_info.city_id = tea_class_info.city_id
AND tut_city_rank_info.tea_id = tea_class_info.tutor_id;

 --教师与辅导教师合并

DROP TABLE IF EXISTS odata_dev.app_xes_years_tea_class_info_join;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_tea_class_info_join AS
SELECT *
FROM odata_dev.app_xes_years_tea_class_info_join_tea
UNION ALL
SELECT *
FROM odata_dev.app_xes_years_tea_class_info_join_tut;









 --关联所有课次表

DROP TABLE IF EXISTS odata_dev.app_xes_years_teacher_cuc_info;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_teacher_cuc_info AS
SELECT tea_class_info.city_id ,
       tea_class_info.cla_id ,
       tea_class_info.tea_id ,
       tea_class_info.venue_id ,
       tea_class_info.venue_name ,
       tea_class_info.lvl_id ,
       tea_class_info.lvl_name ,
       concat(oddc.cuc_date,' ',oddc.cuc_s_time) AS class_time ,
       cuc_id ,
       cuc_no
FROM odata_dev.app_xes_years_tea_class_info_join tea_class_info
JOIN odata_dev.dw_curriculum oddc ON tea_class_info.city_id = oddc.city_id
AND tea_class_info.cla_id = oddc.cla_id
AND oddc.cuc_date<='2018-01-08';

 --第一次课信息

DROP TABLE IF EXISTS odata_dev.app_xes_years_teacher_first_class_info;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_teacher_first_class_info AS
SELECT city_id ,
       tea_id ,
       class_time AS tea_first_class_time ,
       venue_id AS tea_first_class_venue_id ,
       venue_name AS tea_first_class_venue_name ,
       lvl_id AS tea_first_class_cuc_id ,
       lvl_name AS tea_first_class_cuc_name ,
       cast((2018-substr(class_time,1,4)) AS INT) AS tea_teach_years
FROM
 (SELECT city_id ,
         cla_id ,
         tea_id ,
         venue_id ,
         venue_name ,
         lvl_id ,
         lvl_name ,
         class_time ,
         row_number() over(partition BY city_id,tea_id
                           ORDER BY class_time ASC) AS rank_num ,
         cuc_id ,
         cuc_no
  FROM odata_dev.app_xes_years_teacher_cuc_info) TEMP
WHERE rank_num='1';

 ---教授的学生合计

DROP TABLE IF EXISTS odata_dev.app_xes_years_teacher_tea_stu_info;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_teacher_tea_stu_info AS
SELECT city_id ,
       tea_id ,
       count(*) AS tea_stu_nums
FROM
 (SELECT tea_class_info.city_id ,
         tea_class_info.cla_id ,
         tea_class_info.tea_id ,
         student_info.stu_id
  FROM odata_dev.app_xes_years_tea_class_info_join tea_class_info
  JOIN
   (SELECT t.stu_id,
           t.city_id,
           t.city_name,
           t.cla_id
    FROM
     (SELECT stu_id,
             city_id,
             city_name,
             cur_cla_id AS cla_id
      FROM odata_dev.dw_regist
      WHERE pay_flag = '1'
      UNION ALL SELECT stu_id,
                       city_id,
                       city_name,
                       rc_cla_id AS cla_id
      FROM odata_dev.dw_stu_ret_info
      WHERE ret_cla_type = '2') t) student_info ON tea_class_info.city_id = student_info.city_id
  AND tea_class_info.cla_id = student_info.cla_id) TEMP
GROUP BY city_id,
         tea_id;


---教师统计信息
 DROP TABLE IF EXISTS odata_dev.app_xes_years_teacher_statistics_info;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_teacher_statistics_info AS
SELECT city_id ,
       tea_id ,
       count(*) AS tea_cuc_amounts ,
       count(*)*23 AS tea_handout_numbers ,
       round(count(*)*2.6,2) AS tea_class_hourse
FROM odata_dev.app_xes_years_teacher_cuc_info
GROUP BY city_id,
         tea_id;

 --教师努力程度

DROP TABLE IF EXISTS odata_dev.app_xes_years_teacher_strive_info;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_teacher_strive_info AS
SELECT teacher_statistics_info.city_id ,
       teacher_statistics_info.tea_id ,
       concat(cast(round((1-(teacher_statistics_info.tea_strive_rank/city_tea_nums.city_teacher_nums))*100,2) AS string),'%') AS tea_strive_ratio
FROM
 (SELECT city_id ,
         tea_id ,
         tea_cuc_amounts ,
         row_number() over(partition BY city_id
                           ORDER BY tea_cuc_amounts DESC) AS tea_strive_rank
  FROM odata_dev.app_xes_years_teacher_statistics_info) teacher_statistics_info
JOIN
 (SELECT city_id ,
         count(*) city_teacher_nums
  FROM odata_dev.app_xes_years_tea_city_rank_info
  GROUP BY city_id) city_tea_nums ON teacher_statistics_info.city_id = city_tea_nums.city_id;

 ----------------------------------------------------------------------结果--------------------------------------------------------------------

DROP TABLE IF EXISTS odata_dev.app_xes_years_teacher_data;


CREATE TABLE IF NOT EXISTS odata_dev.app_xes_years_teacher_data AS
SELECT concat(tea_city_rank_info.city_id,tea_city_rank_info.tea_id) AS id ,
       tea_city_rank_info.tea_id ,
       regexp_replace(oddt.tea_name,'[0-9,（,）,(,)]','') AS tea_name ,
       tea_city_rank_info.city_id AS tea_city_id ,
       oddt.city_name AS tea_city_name ,
       tea_city_rank_info.tea_city_rank ,
       teacher_first_class_info.tea_first_class_time ,
       teacher_first_class_info.tea_first_class_venue_id ,
       teacher_first_class_info.tea_first_class_venue_name ,
       teacher_first_class_info.tea_first_class_cuc_id ,
       teacher_first_class_info.tea_first_class_cuc_name ,
       teacher_first_class_info.tea_teach_years ,
       tea_stu_info.tea_stu_nums AS tea_teach_student_amounts ,
       teacher_statistics_info.tea_cuc_amounts ,
       teacher_statistics_info.tea_handout_numbers ,
       teacher_statistics_info.tea_class_hourse ,
       teacher_strive_info.tea_strive_ratio
FROM
 (SELECT *
  FROM odata_dev.app_xes_years_tea_city_rank_info
  UNION ALL SELECT *
  FROM odata_dev.app_xes_years_tut_city_rank_info) tea_city_rank_info
JOIN odata_dev.app_xes_years_teacher_first_class_info teacher_first_class_info ON tea_city_rank_info.city_id = teacher_first_class_info.city_id
AND tea_city_rank_info.tea_id = teacher_first_class_info.tea_id
JOIN odata_dev.app_xes_years_teacher_tea_stu_info tea_stu_info ON tea_city_rank_info.city_id = tea_stu_info.city_id
AND tea_city_rank_info.tea_id =tea_stu_info.tea_id
JOIN odata_dev.app_xes_years_teacher_statistics_info teacher_statistics_info ON tea_city_rank_info.city_id = teacher_statistics_info.city_id
AND tea_city_rank_info.tea_id =teacher_statistics_info.tea_id
JOIN odata_dev.app_xes_years_teacher_strive_info teacher_strive_info ON tea_city_rank_info.city_id = teacher_strive_info.city_id
AND tea_city_rank_info.tea_id = teacher_strive_info.tea_id
LEFT JOIN
 (SELECT tea_id,
         city_id,
         city_name,
         tea_name
  FROM odata_dev.dw_teacher
  UNION ALL SELECT tutor_id AS tea_id,
                   city_id,
                   city_name,
                   tutor_real_name AS tea_name
  FROM odata_dev.dw_tutor) oddt ON tea_city_rank_info.city_id = oddt.city_id
AND tea_city_rank_info.tea_id = oddt.tea_id;





INSERT overwrite TABLE otemp.app_xes_years_teacher_test
SELECT *
FROM odata_dev.app_xes_years_teacher_data;


SELECT count(*)
FROM otemp.app_xes_years_teacher_test;

