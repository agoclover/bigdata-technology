-- 列转行
-- split(): 将给定的字符串，通过给定的分隔符进行切割，返回array
-- explode(): 将一列中的array或者map等拆分成多行(炸裂)
-- LATERAL VIEW: 侧写

-- 需求:
 movie	        category2
《疑犯追踪》	悬疑,动作,科幻,剧情
《Lie TO me》	悬疑,警匪,动作,心理,剧情
《战狼2》	战争,动作,灾难

-- 结果:

《疑犯追踪》      悬疑
《疑犯追踪》      动作
《疑犯追踪》      科幻
《疑犯追踪》      剧情
《Lie TO me》     悬疑
《Lie TO me》     警匪
《Lie TO me》     动作
《Lie TO me》     心理
《Lie TO me》     剧情
《战狼2》         战争
《战狼2》         动作
《战狼2》         灾难


-- 分析:
SELECT
movie,
category_name
FROM movie_info 
lateral VIEW
explode(split(category,',')) movie_info_tmp  AS category_name  ;

=============================================================================
-- 窗口函数(开窗函数)
-- over():  
-- 开窗的规则:
-- 在每个窗口中，会为每条数据都打开一个窗口
-- over(): 窗口的大小就是当前查询到的结果集的大小
-- over(partition by) : 窗口的大小就是每个分区中结果集的大小
-- over(order by ):  窗口的大小就是从结果集的开始位置到当前行。
-- over(partition by order by):窗口的大小就是每个分区中从结果集的开始位置到当前行.

-- 几个关键字:
-- 建表: partitioned by (分区表)  clustered by(分桶表)
-- 查询: order by (全局排序)  distribute by(分区)  sort by(区内排序)  cluster by(分区排序)
-- 窗口函数:   partition by(分区) order by(排序)
--              distribute by(分区)  sort by (排序)

-- 数据:
jack,2017-01-01,10
tony,2017-01-02,15
jack,2017-02-03,23
tony,2017-01-04,29
jack,2017-01-05,46
jack,2017-04-06,42
tony,2017-01-07,50
jack,2017-01-08,55
mart,2017-04-08,62
mart,2017-04-09,68
neil,2017-05-10,12
mart,2017-04-11,75
neil,2017-06-12,80
mart,2017-04-13,94


-- 需求一:
（1）查询在2017年4月份购买过的顾客及总人数
     NAME    total  
     jack     2
     tony     2
     
-- 分析:
-- a. 将2017年4月份的数据查出来
SELECT NAME ,orderdate ,cost FROM business WHERE  SUBSTRING(orderdate,1,7) = '2017-04' 
SELECT NAME ,orderdate ,cost FROM business WHERE  orderdate LIKE '2017-04%'
SELECT NAME ,orderdate ,cost FROM business WHERE  MONTH(orderdate) ='04'   ==>t1
+-------+-------------+-------+
| NAME  |  orderdate  | cost  |
+-------+-------------+-------+
| jack  | 2017-04-06  | 42    |
| mart  | 2017-04-08  | 62    |
| mart  | 2017-04-09  | 68    |
| mart  | 2017-04-11  | 75    |
| mart  | 2017-04-13  | 94    |
+-------+-------------+-------+

-- b.  大家的思路: 去重统计  分组  
-- 去重:  
SELECT DISTINCT(t1.name) FROM (SELECT NAME ,orderdate ,cost FROM business WHERE  MONTH(orderdate) ='04')t1  ==>t2 
+----------+
| t1.name  |
+----------+
| jack     |
| mart     |
+----------+
SELECT  t2.name, 
(SELECT COUNT(t2.name) FROM (SELECT DISTINCT(t1.name) FROM (SELECT NAME ,orderdate ,cost FROM business WHERE  MONTH(orderdate) ='04')t1)t2 ) 
FROM (SELECT DISTINCT(t1.name) FROM (SELECT NAME ,orderdate ,cost FROM business WHERE  MONTH(orderdate) ='04')t1)t2 
+----------+------+
| t2.name  | _c1  |
+----------+------+
| jack     | 2    |
| mart     | 2    |
+----------+------+

-- 分组
SELECT t1.name ,COUNT(t1.name)
FROM (SELECT t1.name FROM (SELECT NAME ,orderdate ,cost FROM business WHERE  MONTH(orderdate) ='04')t1)t1
GROUP BY t1.name 
+----------+------+
| t1.name  | _c1  |
+----------+------+
| jack     | 1    |
| mart     | 4    |
+----------+------+


-- 窗口函数的思路
SELECT t1.name , COUNT(*) over()
FROM (SELECT NAME ,orderdate ,cost FROM business WHERE  MONTH(orderdate) ='04' )t1 
GROUP BY t1.name 

+----------+-----------------+
| t1.name  | count_window_0  |
+----------+-----------------+
| mart     | 2               |
| jack     | 2               |
+----------+-----------------+


-- 需求二:
（2）查询顾客的购买明细及月购买总额(所有顾客的月购买总额)
SELECT NAME, orderdate,cost ,
SUM(cost) over(PARTITION BY MONTH(orderdate)) sum_cost
FROM
business
+-------+-------------+-------+-----------+
| NAME  |  orderdate  | cost  | sum_cost  |
+-------+-------------+-------+-----------+
| jack  | 2017-01-01  | 10    | 205       |
| jack  | 2017-01-08  | 55    | 205       |
| tony  | 2017-01-07  | 50    | 205       |
| jack  | 2017-01-05  | 46    | 205       |
| tony  | 2017-01-04  | 29    | 205       |
| tony  | 2017-01-02  | 15    | 205       |
| jack  | 2017-02-03  | 23    | 23        |
| mart  | 2017-04-13  | 94    | 341       |
| jack  | 2017-04-06  | 42    | 341       |
| mart  | 2017-04-11  | 75    | 341       |
| mart  | 2017-04-09  | 68    | 341       |
| mart  | 2017-04-08  | 62    | 341       |
| neil  | 2017-05-10  | 12    | 12        |
| neil  | 2017-06-12  | 80    | 80        |
+-------+-------------+-------+-----------+


 (2) 查询顾客的购买明细及月购买总额(每个顾客的月购买总额)
SELECT NAME, orderdate, cost ,
SUM(cost) over(PARTITION BY NAME, MONTH(orderdate)) sum__cost
 FROM business 
 
 +-------+-------------+-------+------------+
| NAME  |  orderdate  | cost  | sum__cost  |
+-------+-------------+-------+------------+
| jack  | 2017-01-05  | 46    | 111        |
| jack  | 2017-01-08  | 55    | 111        |
| jack  | 2017-01-01  | 10    | 111        |
| jack  | 2017-02-03  | 23    | 23         |
| jack  | 2017-04-06  | 42    | 42         |
| mart  | 2017-04-13  | 94    | 299        |
| mart  | 2017-04-11  | 75    | 299        |
| mart  | 2017-04-09  | 68    | 299        |
| mart  | 2017-04-08  | 62    | 299        |
| neil  | 2017-05-10  | 12    | 12         |
| neil  | 2017-06-12  | 80    | 80         |
| tony  | 2017-01-04  | 29    | 94         |
| tony  | 2017-01-02  | 15    | 94         |
| tony  | 2017-01-07  | 50    | 94         |
+-------+-------------+-------+------------+

-- 需求三:
（3）每个顾客的购买明细,及所有顾客的cost按照日期进行累加
-- 分析: 为每条数据的开窗的大小为: 从结果集开始的位置到当前行.
-- 默认效果: 
SELECT NAME, orderdate,cost, 
SUM(cost) over(ORDER BY orderdate) sum_cost
FROM business 
+-------+-------------+-------+-----------+
| NAME  |  orderdate  | cost  | sum_cost  |
+-------+-------------+-------+-----------+
| jack  | 2017-01-01  | 10    | 10        |
| tony  | 2017-01-02  | 15    | 25        |
| tony  | 2017-01-04  | 29    | 54        |
| jack  | 2017-01-05  | 46    | 100       |
| tony  | 2017-01-07  | 50    | 150       |
| jack  | 2017-01-08  | 55    | 205       |
| jack  | 2017-02-03  | 23    | 228       |
| jack  | 2017-04-06  | 42    | 270       |
| mart  | 2017-04-08  | 62    | 332       |
| mart  | 2017-04-09  | 68    | 400       |
| mart  | 2017-04-11  | 75    | 475       |
| mart  | 2017-04-13  | 94    | 569       |
| neil  | 2017-05-10  | 12    | 581       |
| neil  | 2017-06-12  | 80    | 661       |
+-------+-------------+-------+-----------+

-- UNBOUNDED PRECEDING    CURRENT ROW
SELECT NAME, orderdate,cost, 
SUM(cost) over(ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) sum_cost
FROM business
+-------+-------------+-------+-----------+
| NAME  |  orderdate  | cost  | sum_cost  |
+-------+-------------+-------+-----------+
| jack  | 2017-01-01  | 10    | 10        |
| tony  | 2017-01-02  | 15    | 25        |
| tony  | 2017-01-04  | 29    | 54        |
| jack  | 2017-01-05  | 46    | 100       |
| tony  | 2017-01-07  | 50    | 150       |
| jack  | 2017-01-08  | 55    | 205       |
| jack  | 2017-02-03  | 23    | 228       |
| jack  | 2017-04-06  | 42    | 270       |
| mart  | 2017-04-08  | 62    | 332       |
| mart  | 2017-04-09  | 68    | 400       |
| mart  | 2017-04-11  | 75    | 475       |
| mart  | 2017-04-13  | 94    | 569       |
| neil  | 2017-05-10  | 12    | 581       |
| neil  | 2017-06-12  | 80    | 661       |
+-------+-------------+-------+-----------+

-- 需求三扩展1: 
   每个顾客的购买明细  及
	所有顾客的cost按照日期累加
	所有顾客的cost按照日期上一行与当前行的累加
	所有顾客的cost按照日期当前行与下一行的累加
	所有顾客的cost按照日期上一行 当前行  下一行的累加

SELECT NAME, orderdate ,cost ,
SUM(cost) over(ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_cost1,
SUM(cost) over(ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND  CURRENT ROW ) sum_cost2,
SUM(cost) over(ORDER BY orderdate ROWS BETWEEN CURRENT ROW AND  1 FOLLOWING) sum_cost3, 
SUM(cost) over(ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) sum_cost4 
FROM business	
	

	
-- 需求三扩展2：	
  每个顾客的购买明细 及
       所有顾客的cost按照日期 上一行与下一行的累加
       
-- a. 将每条数据的上一行 和 下一行的cost 提取到当前行
SELECT NAME, orderdate ,cost ,
lag(cost,1,0) over(ORDER BY orderdate) prev_cost,
lead(cost,1,0) over(ORDER BY orderdate) next_cost
FROM business       == >t1

+-------+-------------+-------+------------+------------+
| NAME  |  orderdate  | cost  | prev_cost  | next_cost  |
+-------+-------------+-------+------------+------------+
| jack  | 2017-01-01  | 10    | 0          | 15         |
| tony  | 2017-01-02  | 15    | 10         | 29         |
| tony  | 2017-01-04  | 29    | 15         | 46         |
| jack  | 2017-01-05  | 46    | 29         | 50         |
| tony  | 2017-01-07  | 50    | 46         | 55         |
| jack  | 2017-01-08  | 55    | 50         | 23         |
| jack  | 2017-02-03  | 23    | 55         | 42         |
| jack  | 2017-04-06  | 42    | 23         | 62         |
| mart  | 2017-04-08  | 62    | 42         | 68         |
| mart  | 2017-04-09  | 68    | 62         | 75         |
| mart  | 2017-04-11  | 75    | 68         | 94         |
| mart  | 2017-04-13  | 94    | 75         | 12         |
| neil  | 2017-05-10  | 12    | 94         | 80         |
| neil  | 2017-06-12  | 80    | 12         | 0          |
+-------+-------------+-------+------------+------------+

-- b. 计算prev_cost 与 next_cost的和
SELECT t1.name, t1.orderdate, t1.cost ,t1.prev_cost, t1.next_cost ,
t1.prev_cost + t1.next_cost  p_n_cost
FROM (SELECT NAME, orderdate ,cost ,
lag(cost,1,0) over(ORDER BY orderdate) prev_cost,
lead(cost,1,0) over(ORDER BY orderdate) next_cost
FROM business)t1

+----------+---------------+----------+---------------+---------------+-----------+
| t1.name  | t1.orderdate  | t1.cost  | t1.prev_cost  | t1.next_cost  | p_n_cost  |
+----------+---------------+----------+---------------+---------------+-----------+
| jack     | 2017-01-01    | 10       | 0             | 15            | 15        |
| tony     | 2017-01-02    | 15       | 10            | 29            | 39        |
| tony     | 2017-01-04    | 29       | 15            | 46            | 61        |
| jack     | 2017-01-05    | 46       | 29            | 50            | 79        |
| tony     | 2017-01-07    | 50       | 46            | 55            | 101       |
| jack     | 2017-01-08    | 55       | 50            | 23            | 73        |
| jack     | 2017-02-03    | 23       | 55            | 42            | 97        |
| jack     | 2017-04-06    | 42       | 23            | 62            | 85        |
| mart     | 2017-04-08    | 62       | 42            | 68            | 110       |
| mart     | 2017-04-09    | 68       | 62            | 75            | 137       |
| mart     | 2017-04-11    | 75       | 68            | 94            | 162       |
| mart     | 2017-04-13    | 94       | 75            | 12            | 87        |
| neil     | 2017-05-10    | 12       | 94            | 80            | 174       |
| neil     | 2017-06-12    | 80       | 12            | 0             | 12        |
+----------+---------------+----------+---------------+---------------+-----------+

-- 需求四:
（4）查询每个顾客上次的购买时间 和 下次的购买时间
SELECT NAME, cost , orderdate current_od,
lag(orderdate,1,'1970-01-01') over( PARTITION BY NAME ORDER BY orderdate ) prev_od,
lead(orderdate,1,'9999-01-01') over(PARTITION BY NAME ORDER BY orderdate) next_od
FROM business 
+-------+-------+-------------+-------------+-------------+
| NAME  | cost  | current_od  |   prev_od   |   next_od   |
+-------+-------+-------------+-------------+-------------+
| jack  | 10    | 2017-01-01  | 1970-01-01  | 2017-01-05  |
| jack  | 46    | 2017-01-05  | 2017-01-01  | 2017-01-08  |
| jack  | 55    | 2017-01-08  | 2017-01-05  | 2017-02-03  |
| jack  | 23    | 2017-02-03  | 2017-01-08  | 2017-04-06  |
| jack  | 42    | 2017-04-06  | 2017-02-03  | 9999-01-01  |
| mart  | 62    | 2017-04-08  | 1970-01-01  | 2017-04-09  |
| mart  | 68    | 2017-04-09  | 2017-04-08  | 2017-04-11  |
| mart  | 75    | 2017-04-11  | 2017-04-09  | 2017-04-13  |
| mart  | 94    | 2017-04-13  | 2017-04-11  | 9999-01-01  |
| neil  | 12    | 2017-05-10  | 1970-01-01  | 2017-06-12  |
| neil  | 80    | 2017-06-12  | 2017-05-10  | 9999-01-01  |
| tony  | 15    | 2017-01-02  | 1970-01-01  | 2017-01-04  |
| tony  | 29    | 2017-01-04  | 2017-01-02  | 2017-01-07  |
| tony  | 50    | 2017-01-07  | 2017-01-04  | 9999-01-01  |
+-------+-------+-------------+-------------+-------------+

-- 需求四扩展:
（4）查询每个顾客上次的购买时间 和 下次的购买时间 及每个顾客的cost按照日期累加
SELECT NAME, cost , orderdate current_od,
lag(orderdate,1,'1970-01-01') over( PARTITION BY NAME ORDER BY orderdate ) prev_od,
lead(orderdate,1,'9999-01-01') over(PARTITION BY NAME ORDER BY orderdate) next_od,
SUM(cost) over(PARTITION BY NAME ORDER BY orderdate) sum_cost
FROM business



SELECT NAME, cost , orderdate current_od,
lag(orderdate,1,'1970-01-01') over( distribute BY NAME sort BY orderdate ) prev_od,
lead(orderdate,1,'9999-01-01') over(PARTITION BY NAME ORDER BY orderdate) next_od,
SUM(cost) over(PARTITION BY NAME ORDER BY orderdate) sum_cost
FROM business


+-------+-------+-------------+-------------+-------------+-----------+
| NAME  | cost  | current_od  |   prev_od   |   next_od   | sum_cost  |
+-------+-------+-------------+-------------+-------------+-----------+
| jack  | 10    | 2017-01-01  | 1970-01-01  | 2017-01-05  | 10        |
| jack  | 46    | 2017-01-05  | 2017-01-01  | 2017-01-08  | 56        |
| jack  | 55    | 2017-01-08  | 2017-01-05  | 2017-02-03  | 111       |
| jack  | 23    | 2017-02-03  | 2017-01-08  | 2017-04-06  | 134       |
| jack  | 42    | 2017-04-06  | 2017-02-03  | 9999-01-01  | 176       |
| mart  | 62    | 2017-04-08  | 1970-01-01  | 2017-04-09  | 62        |
| mart  | 68    | 2017-04-09  | 2017-04-08  | 2017-04-11  | 130       |
| mart  | 75    | 2017-04-11  | 2017-04-09  | 2017-04-13  | 205       |
| mart  | 94    | 2017-04-13  | 2017-04-11  | 9999-01-01  | 299       |
| neil  | 12    | 2017-05-10  | 1970-01-01  | 2017-06-12  | 12        |
| neil  | 80    | 2017-06-12  | 2017-05-10  | 9999-01-01  | 92        |
| tony  | 15    | 2017-01-02  | 1970-01-01  | 2017-01-04  | 15        |
| tony  | 29    | 2017-01-04  | 2017-01-02  | 2017-01-07  | 44        |
| tony  | 50    | 2017-01-07  | 2017-01-04  | 9999-01-01  | 94        |
+-------+-------+-------------+-------------+-------------+-----------+


-- 需求五:
（5）查询前20%时间的订单信息

SELECT  NAME, orderdate, cost , 
NTILE(5) over(ORDER BY orderdate) gid
FROM business  ==>t1
+-------+-------------+-------+------+
| NAME  |  orderdate  | cost  | gid  |
+-------+-------------+-------+------+
| jack  | 2017-01-01  | 10    | 1    |
| tony  | 2017-01-02  | 15    | 1    |
| tony  | 2017-01-04  | 29    | 1    |
| jack  | 2017-01-05  | 46    | 2    |
| tony  | 2017-01-07  | 50    | 2    |
| jack  | 2017-01-08  | 55    | 2    |
| jack  | 2017-02-03  | 23    | 3    |
| jack  | 2017-04-06  | 42    | 3    |
| mart  | 2017-04-08  | 62    | 3    |
| mart  | 2017-04-09  | 68    | 4    |
| mart  | 2017-04-11  | 75    | 4    |
| mart  | 2017-04-13  | 94    | 4    |
| neil  | 2017-05-10  | 12    | 5    |
| neil  | 2017-06-12  | 80    | 5    |
+-------+-------------+-------+------+

SELECT t1.name, t1.orderdate ,t1.cost ,t1.gid 
FROM (SELECT  NAME, orderdate, cost , 
NTILE(5) over(ORDER BY orderdate) gid
FROM business)t1 
WHERE t1.gid = 1 

+----------+---------------+----------+---------+
| t1.name  | t1.orderdate  | t1.cost  | t1.gid  |
+----------+---------------+----------+---------+
| jack     | 2017-01-01    | 10       | 1       |
| tony     | 2017-01-02    | 15       | 1       |
| tony     | 2017-01-04    | 29       | 1       |
+----------+---------------+----------+---------+

=====================================================================

-- 排名
-- rank() : 考虑并列情况, 总数不会变
-- dense_rank(): 考虑并列情况，总数会减少
-- row_number(): 按照顺序编号
SELECT NAME, SUBJECT ,score ,
rank() over(PARTITION BY SUBJECT ORDER BY score DESC) rk,
dense_rank() over(PARTITION BY SUBJECT ORDER BY score DESC) drk,
row_number() over(PARTITION BY SUBJECT ORDER BY score DESC) rn 
FROM score 

+-------+----------+--------+-----+------+-----+
| NAME  | SUBJECT  | score  | rk  | drk  | rn  |
+-------+----------+--------+-----+------+-----+
| 孙悟空  | 数学       | 95     | 1   | 1    | 1   |
| 宋宋    | 数学       | 86     | 2   | 2    | 2   |
| 婷婷    | 数学       | 85     | 3   | 3    | 3   |
| 大海    | 数学       | 56     | 4   | 4    | 4   |
| 宋宋    | 英语       | 84     | 1   | 1    | 1   |
| 大海    | 英语       | 84     | 1   | 1    | 2   |
| 婷婷    | 英语       | 78     | 3   | 2    | 3   |
| 孙悟空  | 英语       | 68     | 4   | 3    | 4   |
| 大海    | 语文       | 94     | 1   | 1    | 1   |
| 孙悟空  | 语文       | 87     | 2   | 2    | 2   |
| 婷婷    | 语文       | 65     | 3   | 3    | 3   |
| 宋宋    | 语文       | 64     | 4   | 4    | 4   |
+-------+----------+--------+-----+------+-----+



====================================================================

-- 自定义函数
-- 自定义UDF函数
   1. 自定义类 ，继承GenericUDF类，重写initialize  和 evaluate 方法
   2. 打包
   3. 将打好的包传到linux中
   4. 将打好的jar包添加到hive的环境中 
      -- add jar  xxx.jar
   5. 创建函数
      -- create temporary function  xxxx  as "自定义的UDF的全类名"; 
      
-- 自定义UDTF函数
   1. 自定义类，继承GenericUDTF类，重写initialize 和 PROCESS 方法   
   2. 打包
   3. 将打好的包传到linux中国
   4. 将打好的jar包添加到hive的环境中
      -- add jar  xxx.jar
   5. 创建函数
      -- create temporary function  xxxx  as "自定义的UDF的全类名";    
      


作业: 
1. 函数: 
   行转列  列转行
   窗口函数五个需求及扩展
   排名函数  
   其他常用函数 
   自定义函数
   
2. 课后练习三道题   
       







SET hive.exec.mode.local.auto=TRUE; 	












