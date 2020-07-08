-- 5.2.1
insert overwrite directory '/export'
row format delimited fields terminated by '/t'
select * from student;

-- 5.2.2
dfs -get /user/hive/warehouse/mydb.db/student /opt/module/hive/data/export/student3.txt;

-- 5.2.3
hive -e "select * from mydb.student;" > /opt/module/hive/data/export/student4.txt;

-- 5.2.4
export table mydb.student to '/export/emp/';

-- 5.1.5
import table mydb.student_515 from '/export/emp';

-- 5.2.6
truncate table student_515;