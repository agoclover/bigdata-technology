-- 5.2.1
insert overwrite directory '/export'
row format delimited fields terminated by '/t'
select * from student;

--5.2.2
