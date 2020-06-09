-- 谷粒影音实战
-- 两个表
-- 视频表: gulivideo_orc

字段		备注				详细描述

videoId		视频唯一id（String）		11位字符串
uploader	视频上传者（String）		上传视频的用户名String
age		视频年龄（int）			视频在平台上的整数天
category	视频类别（Array<STRING>）	上传视频指定的视频分类
LENGTH		视频长度（Int）			整形数字标识的视频长度
views		观看次数（Int）			视频被浏览的次数
rate		视频评分（Double）		满分5分
Ratings		流量（Int）			视频的流量，整型数字
conments	评论数（Int）			一个视频的整数评论数
relatedId	相关视频id（Array<STRING>）	相关视频的id，最多20个

-- 用户表: gulivideo_user_orc

字段		备注		字段类型
uploader	上传者用户名	STRING
videos		上传视频数	INT
friends		朋友数量	INT
==================================================================================
-- 需求一: 统计视频观看数Top10
SELECT videoId, views 
FROM  gulivideo_orc
ORDER BY views DESC 
LIMIT 10 ;

+--------------+-----------+
|   videoid    |   views   |
+--------------+-----------+
| dMH0bHeiRNg  | 42513417  |
| 0XxI-hvPRRA  | 20282464  |
| 1dmVU08zVpA  | 16087899  |
| RB-wUgnyGv0  | 15712924  |
| QjA5faZF1A8  | 15256922  |
| -_CSo1gOd48  | 13199833  |
| 49IDp76kjPw  | 11970018  |
| tYnn51C3X_w  | 11823701  |
| pv5zWaTEVkI  | 11672017  |
| D2kJZOfq7zk  | 11184051  |
+--------------+-----------+

======================================================================
-- 需求二: 统计视频类别热度Top10
-- 热度: 总视频数   总流量   总观看数.....
-- 约定: 按照类别下包含的总视频个数来衡量热度

-- a. 炸开每个视频的类别
SELECT videoId , category_name
FROM gulivideo_orc 
lateral VIEW explode(category) gulivideo_orc_tmp AS category_name  ==>t1 

-- b. 按照category_name 分组 ,count每个组视频的个数，倒叙，limit10个。
SELECT t1.category_name , COUNT(t1.videoId) hot
FROM t1
GROUP BY t1.category_name
ORDER BY hot DESC 
LIMIT 10 

-- c. 组合:
SELECT t1.category_name , COUNT(t1.videoId) hot
FROM (SELECT videoId , category_name
FROM gulivideo_orc 
lateral VIEW explode(category) gulivideo_orc_tmp AS category_name)t1
GROUP BY t1.category_name
ORDER BY hot DESC 
LIMIT 10 

+-------------------+---------+
| t1.category_name  |   hot   |
+-------------------+---------+
| Music             | 179049  |
| Entertainment     | 127674  |
| Comedy            | 87818   |
| Animation         | 73293   |
| Film              | 73293   |
| Sports            | 67329   |
| Games             | 59817   |
| Gadgets           | 59817   |
| People            | 48890   |
| Blogs             | 48890   |
+-------------------+---------+

======================================================================

-- 需求三: 统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数
-- a. 统计出视频观看数最高的20个视频的所属类别
SELECT videoId, category,views
FROM gulivideo_orc
ORDER BY views DESC
LIMIT 20   ==>t1

+--------------+---------------------+
|   videoid    |      category       |
+--------------+---------------------+
| dMH0bHeiRNg  | ["Comedy"]          |
| 0XxI-hvPRRA  | ["Comedy"]          |
| 1dmVU08zVpA  | ["Entertainment"]   |
| RB-wUgnyGv0  | ["Entertainment"]   |
| QjA5faZF1A8  | ["Music"]           |
| -_CSo1gOd48  | ["People","Blogs"]  |
| 49IDp76kjPw  | ["Comedy"]          |
| tYnn51C3X_w  | ["Music"]           |
| pv5zWaTEVkI  | ["Music"]           |
| D2kJZOfq7zk  | ["People","Blogs"]  |
| vr3x_RRJdd4  | ["Entertainment"]   |
| lsO6D1rwrKc  | ["Entertainment"]   |
| 5P6UU6m3cqk  | ["Comedy"]          |
| 8bbTtPL1jRs  | ["Music"]           |
| _BuRwH59oAo  | ["Comedy"]          |
| aRNzWyD7C9o  | ["UNA"]             |
| UMf40daefsI  | ["Music"]           |
| ixsZy2425eY  | ["Entertainment"]   |
| MNxwAU_xAMk  | ["Comedy"]          |
| RUCZJVJ_M8o  | ["Entertainment"]   |
+--------------+---------------------+

-- b.炸开每个视频的类别
SELECT t1.videoId, category_name
FROM t1 
lateral VIEW explode(t1.category)  gulivideo_orc_tmp AS category_name  ==>t2

-- 组合
SELECT t1.videoId, category_name
FROM (SELECT videoId, category ,views
FROM gulivideo_orc
ORDER BY views DESC
LIMIT 20 )t1 
lateral VIEW explode(t1.category)  gulivideo_orc_tmp AS category_name
|  t1.videoid  | category_name  |
+--------------+----------------+
| dMH0bHeiRNg  | Comedy         |
| 0XxI-hvPRRA  | Comedy         |
| 1dmVU08zVpA  | Entertainment  |
| RB-wUgnyGv0  | Entertainment  |
| QjA5faZF1A8  | Music          |
| -_CSo1gOd48  | People         |
| -_CSo1gOd48  | Blogs          |
| 49IDp76kjPw  | Comedy         |
| tYnn51C3X_w  | Music          |
| pv5zWaTEVkI  | Music          |
| D2kJZOfq7zk  | People         |
| D2kJZOfq7zk  | Blogs          |
| vr3x_RRJdd4  | Entertainment  |
| lsO6D1rwrKc  | Entertainment  |
| 5P6UU6m3cqk  | Comedy         |
| 8bbTtPL1jRs  | Music          |
| _BuRwH59oAo  | Comedy         |
| aRNzWyD7C9o  | UNA            |
| UMf40daefsI  | Music          |
| ixsZy2425eY  | Entertainment  |
| MNxwAU_xAMk  | Comedy         |
| RUCZJVJ_M8o  | Entertainment  |
+--------------+----------------+

-- c. 按照类别分组，求每个类别中的视频个数
SELECT t2.category_name, COUNT(t2.videoId) videos 
FROM t2 
GROUP BY t2.category_name


-- d. 组合
SELECT t2.category_name, COUNT(t2.videoId) videos 
FROM (SELECT t1.videoId, category_name
FROM (SELECT videoId, category ,views
FROM gulivideo_orc
ORDER BY views DESC
LIMIT 20 )t1 
lateral VIEW explode(t1.category)  gulivideo_orc_tmp AS category_name)t2 
GROUP BY t2.category_name
+-------------------+---------+
| t2.category_name  | videos  |
+-------------------+---------+
| Blogs             | 2       |
| Comedy            | 6       |
| Entertainment     | 6       |
| Music             | 5       |
| People            | 2       |
| UNA               | 1       |
+-------------------+---------+
==================================================================

-- 需求四: 统计视频观看数Top50的视频所关联视频的所属类别Rank

-- a. 视频观看数Top50的视频所关联视频
SELECT videoId, relatedId,views
FROM gulivideo_orc
ORDER BY views DESC 
LIMIT 50  ==>t1

+--------------+----------------------------------------------------+
|   videoid    |                     relatedid                      |
+--------------+----------------------------------------------------+
| dMH0bHeiRNg  | ["OxBtqwlTMJQ","1hX1LxXwdl8","NvVbuVGtGSE","Ft6fC6RI4Ms","plv1e3MvxFw","1VL-ShAEjmg","y8k5QbVz3SE","weRfgj_349Q","_MFpPziLP9o","0M-xqfP1ibo","n4Pr_iCxxGU","UrWnNAMec98","QoREX_TLtZo","I-cm3GF-jX0","doIQXfJvydY","6hD3gGg9jMk","Hfbzju1FluI","vVN_pLl5ngg","3PnoFu027hc","7nrpwEDvusY"] |
..............

-- b. 炸开关联视频
SELECT relatedId_id
FROM t1 
lateral VIEW explode(t1.relatedId) gulivideo_orc_tmp AS  relatedId_id  ==>t2

-- 组合
SELECT relatedId_id
FROM (SELECT videoId, relatedId,views
FROM gulivideo_orc
ORDER BY views DESC 
LIMIT 50 ) t1 
lateral VIEW explode(t1.relatedId) gulivideo_orc_tmp AS  relatedId_id

-- c. t2 join  gulivideo_orc表，找到每个关联视频对应的类别
SELECT t2.relatedId_id, t3.category 
FROM t2  JOIN gulivideo_orc t3 
ON t2.relatedId_id = t3. videoId   ==>t4


-- 组合:
-- hive.auto.convert.join = false

SELECT t2.relatedId_id, t3.category 
FROM (SELECT relatedId_id
FROM (SELECT videoId, relatedId,views
FROM gulivideo_orc
ORDER BY views DESC 
LIMIT 50 ) t1 
lateral VIEW explode(t1.relatedId) gulivideo_orc_tmp AS  relatedId_id)t2 JOIN gulivideo_orc t3 
ON t2.relatedId_id = t3.videoId ;


-- d. 炸开类别
SELECT t4.relatedId_id ,category_name
FROM t4
lateral VIEW explode(t4.category) t4_tmp AS category_name  ==>t5

-- e. 按照类别分组， 求每个类别出现的次数
SELECT t5.category_name, COUNT(t5.relatedId_id) category_count
FROM t5
GROUP BY t5.category_name  ==> t6

-- f. 排名
SELECT t6.category_name, t6.category_count ,
rank() over(ORDER BY t6.category_count DESC ) rk
FROM t6 


-- g.组合
SELECT t6.category_name, t6.category_count ,
rank() over(ORDER BY t6.category_count DESC ) rk
FROM (SELECT t5.category_name, COUNT(t5.relatedId_id) category_count
FROM (SELECT t4.relatedId_id ,category_name
FROM (SELECT t2.relatedId_id, t3.category 
FROM (SELECT relatedId_id
FROM (SELECT videoId, relatedId,views
FROM gulivideo_orc
ORDER BY views DESC 
LIMIT 50 ) t1 
lateral VIEW explode(t1.relatedId) gulivideo_orc_tmp AS  relatedId_id)t2 JOIN gulivideo_orc t3 
ON t2.relatedId_id = t3.videoId )t4
lateral VIEW explode(t4.category) t4_tmp AS category_name)t5
GROUP BY t5.category_name )t6 ;


+-------------------+--------------------+-----+
| t6.category_name  | t6.category_count  | rk  |
+-------------------+--------------------+-----+
| Comedy            | 237                | 1   |
| Entertainment     | 216                | 2   |
| Music             | 195                | 3   |
| Blogs             | 51                 | 4   |
| People            | 51                 | 4   |
| Film              | 47                 | 6   |
| Animation         | 47                 | 6   |
| Politics          | 24                 | 8   |
| News              | 24                 | 8   |
| Gadgets           | 22                 | 10  |
| Games             | 22                 | 10  |
| Sports            | 19                 | 12  |
| DIY               | 14                 | 13  |
| Howto             | 14                 | 13  |
| UNA               | 13                 | 15  |
| Travel            | 12                 | 16  |
| Places            | 12                 | 16  |
| Animals           | 11                 | 18  |
| Pets              | 11                 | 18  |
| Autos             | 4                  | 20  |
| Vehicles          | 4                  | 20  |
+-------------------+--------------------+-----+

==============================================================

-- 需求五: 统计每个类别中的视频热度Top10,以Music为例
-- a. 炸开每个视频的类别
SELECT videoId, views, category_name
FROM gulivideo_orc
lateral VIEW explode(category) gulivideo_orc_tmp AS category_name  ==>t1

-- b. 过滤Music类别的数据，按照views倒叙，取top10
SELECT t1.videoId, t1.views, t1.category_name
FROM t1 
WHERE category_name = 'Music'
ORDER BY t1.views DESC 
LIMIT 10 
-- c. 组合
SELECT t1.videoId, t1.views, t1.category_name
FROM (SELECT videoId, views, category_name
FROM gulivideo_orc
lateral VIEW explode(category) gulivideo_orc_tmp AS category_name )t1 
WHERE category_name = 'Music'
ORDER BY t1.views DESC 
LIMIT 10 
+--------------+-----------+-------------------+
|  t1.videoid  | t1.views  | t1.category_name  |
+--------------+-----------+-------------------+
| QjA5faZF1A8  | 15256922  | Music             |
| tYnn51C3X_w  | 11823701  | Music             |
| pv5zWaTEVkI  | 11672017  | Music             |
| 8bbTtPL1jRs  | 9579911   | Music             |
| UMf40daefsI  | 7533070   | Music             |
| -xEzGIuY7kw  | 6946033   | Music             |
| d6C0bNDqf3Y  | 6935578   | Music             |
| HSoVKUVOnfQ  | 6193057   | Music             |
| 3URfWTEPmtE  | 5581171   | Music             |
| thtmaZnxk_0  | 5142238   | Music             |
+--------------+-----------+-------------------+
===========================================================
-- 需求五扩展: 统计每个类别中的视频热度Top10
-- a. 炸开每个视频的类别
SELECT videoId, views, category_name
FROM gulivideo_orc
lateral VIEW explode(category) gulivideo_orc_tmp AS category_name  ==>t1 

-- b. 开窗， 按照列别分区，观看数倒叙排序， 求排名
SELECT t1.videoId,t1.views, t1.category_name ,
rank() over(PARTITION BY t1.category_name ORDER BY t1.views DESC ) rk 
FROM t1  ==>t2

-- c. 取出每个类别的top10
SELECT t2.videoId, t2.views ,t2.category_name, t2.rk
FROM t2
WHERE t2.rk <=10 
 
-- d.组合

SELECT t2.videoId, t2.views ,t2.category_name, t2.rk
FROM (SELECT t1.videoId,t1.views, t1.category_name ,
rank() over(PARTITION BY t1.category_name ORDER BY t1.views DESC ) rk 
FROM (SELECT videoId, views, category_name
FROM gulivideo_orc
lateral VIEW explode(category) gulivideo_orc_tmp AS category_name)t1)t2
WHERE t2.rk <=10  ;

+--------------+-----------+-------------------+--------+
|  t2.videoid  | t2.views  | t2.category_name  | t2.rk  |
+--------------+-----------+-------------------+--------+
| 1dmVU08zVpA  | 16087899  | Entertainment     | 1      |
| RB-wUgnyGv0  | 15712924  | Entertainment     | 2      |
| vr3x_RRJdd4  | 10786529  | Entertainment     | 3      |
| lsO6D1rwrKc  | 10334975  | Entertainment     | 4      |
| ixsZy2425eY  | 7456875   | Entertainment     | 5      |
| RUCZJVJ_M8o  | 6952767   | Entertainment     | 6      |
| tFXLbXyXy6M  | 5810013   | Entertainment     | 7      |
| 7uwCEnDgd5o  | 5280504   | Entertainment     | 8      |
| 2KrdBUFeFtY  | 4676195   | Entertainment     | 9      |
| vD4OnHCRd_4  | 4230610   | Entertainment     | 10     |
| hr23tpWX8lM  | 4706030   | News              | 1      |
| YgW7or1TuFk  | 2899397   | News              | 2      |
| nda_OSWeyn8  | 2817078   | News              | 3      |
| 7SV2sfoPAY8  | 2803520   | News              | 4      |
| HBa9wdOANHw  | 2348709   | News              | 5      |
| xDh_pvv1tUM  | 2335060   | News              | 6      |
| p_YMigZmUuk  | 2326680   | News              | 7      |
| QCVxQ_3Ejkg  | 2318782   | News              | 8      |
| a9WB_PXjTBo  | 2310583   | News              | 9      |
| qSM_3fyiaxM  | 2291369   | News              | 10     |
| sdUUx5FdySs  | 5840839   | Film              | 1      |
| 6B26asyGKDo  | 5147533   | Film              | 2      |
| H20dhY01Xjk  | 3772116   | Film              | 3      |
| 55YYaJIrmzo  | 3356163   | Film              | 4      |
| JzqumbhfxRo  | 3230774   | Film              | 5      |
| eAhfZUZiwSE  | 3114215   | Film              | 6      |
| h7svw0m-wO0  | 2866490   | Film              | 7      |
| tAq3hWBlalU  | 2830024   | Film              | 8      |
| AJzU3NjDikY  | 2569611   | Film              | 9      |
| ElrldD02if0  | 2337238   | Film              | 10     |
| aRNzWyD7C9o  | 8825788   | UNA               | 1      |
| jtExxsiLgPM  | 5320895   | UNA               | 2      |
| PxNNR4symuE  | 4033376   | UNA               | 3      |
| 8cjTSvvoddc  | 3486368   | UNA               | 4      |
| LIhbap3FlGc  | 2849832   | UNA               | 5      |
| lCSTULqmmYE  | 2179562   | UNA               | 6      |
| UyTxWvp8upM  | 2106933   | UNA               | 7      |
| y6oXEWowirI  | 1666084   | UNA               | 8      |
| _x2-AmY8FI8  | 1403113   | UNA               | 9      |
| ICoDFooBXpU  | 1376215   | UNA               | 10     |
| -_CSo1gOd48  | 13199833  | Blogs             | 1      |
| D2kJZOfq7zk  | 11184051  | Blogs             | 2      |
| pa_7P5AbUww  | 5705136   | Blogs             | 3      |
| f4B-r8KJhlE  | 4937616   | Blogs             | 4      |
| LB84A3zcmVo  | 4866739   | Blogs             | 5      |
| tXNquTYnyg0  | 3613323   | Blogs             | 6      |
| EYppbbbSxjc  | 2896562   | Blogs             | 7      |
| LH7vrLlDZ6U  | 2615359   | Blogs             | 8      |
| bTV85fQhj0E  | 2192656   | Blogs             | 9      |
| eVFF98kNg8Q  | 1813803   | Blogs             | 10     |
| 2GWPOPSXGYI  | 3660009   | Animals           | 1      |
| xmsV9R8FsDA  | 3164582   | Animals           | 2      |
| 12PsUW-8ge4  | 3133523   | Animals           | 3      |
| OeNggIGSKH8  | 2457750   | Animals           | 4      |
| WofFb_eOxxA  | 2075728   | Animals           | 5      |
| AgEmZ39EtFk  | 1999469   | Animals           | 6      |
| a-gW3RbJd8U  | 1836870   | Animals           | 7      |
| 8CL2hetqpfg  | 1646808   | Animals           | 8      |
| QmroaYVD_so  | 1645984   | Animals           | 9      |
| Sg9x5mUjbH8  | 1527238   | Animals           | 10     |
| RjrEQaG5jPM  | 2803140   | Autos             | 1      |
| cv157ZIInUk  | 2773979   | Autos             | 2      |
| Gyg9U1YaVk8  | 1832224   | Autos             | 3      |
| 6GNB7xT3rNE  | 1412497   | Autos             | 4      |
| tth9krDtxII  | 1347317   | Autos             | 5      |
| 46LQd9dXFRU  | 1262173   | Autos             | 6      |
| pdiuDXwgrjQ  | 1013697   | Autos             | 7      |
| kY_cDpENQLE  | 956665    | Autos             | 8      |
| YtxfbxGz1u4  | 942604    | Autos             | 9      |
| aCamHfJwSGU  | 847442    | Autos             | 10     |
| hut3VRL5XRE  | 2684989   | DIY               | 1      |
| YYTpb-QXV0k  | 2492153   | DIY               | 2      |
| Pf3z935R37E  | 2096661   | DIY               | 3      |
| Yd99gyE4jCk  | 1918946   | DIY               | 4      |
| koQFjKwVFB0  | 1757071   | DIY               | 5      |
| f5Fg6KFcOsU  | 1751817   | DIY               | 6      |
| STQ3nhXuuEM  | 1713974   | DIY               | 7      |
| FtKuBKIaVvs  | 1520774   | DIY               | 8      |
| M0ODskdEPnQ  | 1503351   | DIY               | 9      |
| uFwCk4UPtlM  | 1500110   | DIY               | 10     |
| hr23tpWX8lM  | 4706030   | Politics          | 1      |
| YgW7or1TuFk  | 2899397   | Politics          | 2      |
| nda_OSWeyn8  | 2817078   | Politics          | 3      |
| 7SV2sfoPAY8  | 2803520   | Politics          | 4      |
| HBa9wdOANHw  | 2348709   | Politics          | 5      |
| xDh_pvv1tUM  | 2335060   | Politics          | 6      |
| p_YMigZmUuk  | 2326680   | Politics          | 7      |
| QCVxQ_3Ejkg  | 2318782   | Politics          | 8      |
| a9WB_PXjTBo  | 2310583   | Politics          | 9      |
| qSM_3fyiaxM  | 2291369   | Politics          | 10     |
| RjrEQaG5jPM  | 2803140   | Vehicles          | 1      |
| cv157ZIInUk  | 2773979   | Vehicles          | 2      |
| Gyg9U1YaVk8  | 1832224   | Vehicles          | 3      |
| 6GNB7xT3rNE  | 1412497   | Vehicles          | 4      |
| tth9krDtxII  | 1347317   | Vehicles          | 5      |
| 46LQd9dXFRU  | 1262173   | Vehicles          | 6      |
| pdiuDXwgrjQ  | 1013697   | Vehicles          | 7      |
| kY_cDpENQLE  | 956665    | Vehicles          | 8      |
| YtxfbxGz1u4  | 942604    | Vehicles          | 9      |
| aCamHfJwSGU  | 847442    | Vehicles          | 10     |
| pFlcqWQVVuU  | 3651600   | Gadgets           | 1      |
| bcu8ZdJ2dQo  | 2617568   | Gadgets           | 2      |
| -G7h626wJwM  | 2565170   | Gadgets           | 3      |
| oMaTZFCLbq0  | 2554620   | Gadgets           | 4      |
| GxSdKF5Fd38  | 2468395   | Gadgets           | 5      |
| z1lj87UyvfY  | 2373875   | Gadgets           | 6      |
| KhCmfX_PQ7E  | 1967929   | Gadgets           | 7      |
| 2SVMFCZgvNM  | 1813794   | Gadgets           | 8      |
| gPutYwiiE0o  | 1633482   | Gadgets           | 9      |
| 7wt5FiZQrgM  | 1399531   | Gadgets           | 10     |
| QjA5faZF1A8  | 15256922  | Music             | 1      |
| tYnn51C3X_w  | 11823701  | Music             | 2      |
| pv5zWaTEVkI  | 11672017  | Music             | 3      |
| 8bbTtPL1jRs  | 9579911   | Music             | 4      |
| UMf40daefsI  | 7533070   | Music             | 5      |
| -xEzGIuY7kw  | 6946033   | Music             | 6      |
| d6C0bNDqf3Y  | 6935578   | Music             | 7      |
| HSoVKUVOnfQ  | 6193057   | Music             | 8      |
| 3URfWTEPmtE  | 5581171   | Music             | 9      |
| thtmaZnxk_0  | 5142238   | Music             | 10     |
| bNF_P281Uu4  | 5231539   | Travel            | 1      |
| s5ipz_0uC_U  | 1198840   | Travel            | 2      |
| 6jJW7aSNCzU  | 1143287   | Travel            | 3      |
| dVRUBIyRAYk  | 1000309   | Travel            | 4      |
| lqbt6X4ZgEI  | 921593    | Travel            | 5      |
| RIH1I1doUI4  | 879577    | Travel            | 6      |
| AlPqL7IUT6M  | 845180    | Travel            | 7      |
| _5QUdvUhCZc  | 819974    | Travel            | 8      |
| m9A_vxIOB-I  | 677876    | Travel            | 9      |
| CL6f3Cyh85w  | 611786    | Travel            | 10     |
| pFlcqWQVVuU  | 3651600   | Games             | 1      |
| bcu8ZdJ2dQo  | 2617568   | Games             | 2      |
| -G7h626wJwM  | 2565170   | Games             | 3      |
| oMaTZFCLbq0  | 2554620   | Games             | 4      |
| GxSdKF5Fd38  | 2468395   | Games             | 5      |
| z1lj87UyvfY  | 2373875   | Games             | 6      |
| KhCmfX_PQ7E  | 1967929   | Games             | 7      |
| 2SVMFCZgvNM  | 1813794   | Games             | 8      |
| gPutYwiiE0o  | 1633482   | Games             | 9      |
| 7wt5FiZQrgM  | 1399531   | Games             | 10     |
| sdUUx5FdySs  | 5840839   | Animation         | 1      |
| 6B26asyGKDo  | 5147533   | Animation         | 2      |
| H20dhY01Xjk  | 3772116   | Animation         | 3      |
| 55YYaJIrmzo  | 3356163   | Animation         | 4      |
| JzqumbhfxRo  | 3230774   | Animation         | 5      |
| eAhfZUZiwSE  | 3114215   | Animation         | 6      |
| h7svw0m-wO0  | 2866490   | Animation         | 7      |
| tAq3hWBlalU  | 2830024   | Animation         | 8      |
| AJzU3NjDikY  | 2569611   | Animation         | 9      |
| ElrldD02if0  | 2337238   | Animation         | 10     |
| dMH0bHeiRNg  | 42513417  | Comedy            | 1      |
| 0XxI-hvPRRA  | 20282464  | Comedy            | 2      |
| 49IDp76kjPw  | 11970018  | Comedy            | 3      |
| 5P6UU6m3cqk  | 10107491  | Comedy            | 4      |
| _BuRwH59oAo  | 9566609   | Comedy            | 5      |
| MNxwAU_xAMk  | 7066676   | Comedy            | 6      |
| pYak2F1hUYA  | 6322117   | Comedy            | 7      |
| h0zAlXr1UOs  | 5826923   | Comedy            | 8      |
| C8rjr4jmWd0  | 5587299   | Comedy            | 9      |
| R4cQ3BoHFas  | 5508079   | Comedy            | 10     |
| Ugrlzm7fySE  | 2867888   | Sports            | 1      |
| q8t7iSGAKik  | 2735003   | Sports            | 2      |
| 7vL19q8yL54  | 2527713   | Sports            | 3      |
| g3dXfFZ6SH0  | 2295871   | Sports            | 4      |
| P-bWsOK-h98  | 2268107   | Sports            | 5      |
| HD8f_Qgwc50  | 2165475   | Sports            | 6      |
| qjWQNwv-GJ4  | 2132591   | Sports            | 7      |
| eN0V-rJQSHE  | 2124653   | Sports            | 8      |
| fM38G1450Ew  | 2052778   | Sports            | 9      |
| 3PGzrfE8rJg  | 2013466   | Sports            | 10     |
| hut3VRL5XRE  | 2684989   | Howto             | 1      |
| YYTpb-QXV0k  | 2492153   | Howto             | 2      |
| Pf3z935R37E  | 2096661   | Howto             | 3      |
| Yd99gyE4jCk  | 1918946   | Howto             | 4      |
| koQFjKwVFB0  | 1757071   | Howto             | 5      |
| f5Fg6KFcOsU  | 1751817   | Howto             | 6      |
| STQ3nhXuuEM  | 1713974   | Howto             | 7      |
| FtKuBKIaVvs  | 1520774   | Howto             | 8      |
| M0ODskdEPnQ  | 1503351   | Howto             | 9      |
| uFwCk4UPtlM  | 1500110   | Howto             | 10     |
| bNF_P281Uu4  | 5231539   | Places            | 1      |
| s5ipz_0uC_U  | 1198840   | Places            | 2      |
| 6jJW7aSNCzU  | 1143287   | Places            | 3      |
| dVRUBIyRAYk  | 1000309   | Places            | 4      |
| lqbt6X4ZgEI  | 921593    | Places            | 5      |
| RIH1I1doUI4  | 879577    | Places            | 6      |
| AlPqL7IUT6M  | 845180    | Places            | 7      |
| _5QUdvUhCZc  | 819974    | Places            | 8      |
| m9A_vxIOB-I  | 677876    | Places            | 9      |
| CL6f3Cyh85w  | 611786    | Places            | 10     |
| -_CSo1gOd48  | 13199833  | People            | 1      |
| D2kJZOfq7zk  | 11184051  | People            | 2      |
| pa_7P5AbUww  | 5705136   | People            | 3      |
| f4B-r8KJhlE  | 4937616   | People            | 4      |
| LB84A3zcmVo  | 4866739   | People            | 5      |
| tXNquTYnyg0  | 3613323   | People            | 6      |
| EYppbbbSxjc  | 2896562   | People            | 7      |
| LH7vrLlDZ6U  | 2615359   | People            | 8      |
| bTV85fQhj0E  | 2192656   | People            | 9      |
| eVFF98kNg8Q  | 1813803   | People            | 10     |
| 2GWPOPSXGYI  | 3660009   | Pets              | 1      |
| xmsV9R8FsDA  | 3164582   | Pets              | 2      |
| 12PsUW-8ge4  | 3133523   | Pets              | 3      |
| OeNggIGSKH8  | 2457750   | Pets              | 4      |
| WofFb_eOxxA  | 2075728   | Pets              | 5      |
| AgEmZ39EtFk  | 1999469   | Pets              | 6      |
| a-gW3RbJd8U  | 1836870   | Pets              | 7      |
| 8CL2hetqpfg  | 1646808   | Pets              | 8      |
| QmroaYVD_so  | 1645984   | Pets              | 9      |
| Sg9x5mUjbH8  | 1527238   | Pets              | 10     |
+--------------+-----------+-------------------+--------+

=====================================================================
-- 需求六: 统计每个类别视频观看数Top10
======================================================================
-- 需求七:统计上传视频最多的用户Top10以及他们上传的视频观看次数在前20的视频 
-- 约定1: 取Top10中每个人上传的视频的前20 
-- a. top10用户
SELECT uploader, videos
FROM gulivideo_user_orc
ORDER BY videos DESC 
LIMIT 10   ==>t1

-- b. 关联gulivideo_orc ,取到这10个用户上传的视频
SELECT t2.uploader, t2.videoId, t2.views
FROM t1 JOIN gulivideo_orc  t2
ON t1.uploader =  t2.uploader  ==>t3

-- c. 开窗， 按照uploader分区， views排序， 求排名
SELECT t3.uploader, t3.videoId, t3.views ,
rank() over(PARTITION BY t3.uploader ORDER BY t3.views DESC ) rk 
FROM t3  ==> t4

-- d. 取每个用户的前20
SELECT t4.uploader ,t4.videoId, t4.views , t4.rk 
FROM t4 
WHERE t4.rk <=20

-- e. 组合

SELECT t4.uploader ,t4.videoId, t4.views , t4.rk 
FROM (SELECT t3.uploader, t3.videoId, t3.views ,
rank() over(PARTITION BY t3.uploader ORDER BY t3.views DESC ) rk 
FROM (SELECT t2.uploader, t2.videoId, t2.views
FROM (SELECT uploader, videos
FROM gulivideo_user_orc
ORDER BY videos DESC 
LIMIT 10)t1 JOIN gulivideo_orc  t2
ON t1.uploader =  t2.uploader )t3 )t4 
WHERE t4.rk <=20

+----------------+--------------+-----------+--------+
|  t4.uploader   |  t4.videoid  | t4.views  | t4.rk  |
+----------------+--------------+-----------+--------+
| expertvillage  | -IxHBW0YpZw  | 39059     | 1      |
| expertvillage  | BU-fT5XI_8I  | 29975     | 2      |
| expertvillage  | ADOcaBYbMl0  | 26270     | 3      |
| expertvillage  | yAqsULIDJFE  | 25511     | 4      |
| expertvillage  | vcm-t0TJXNg  | 25366     | 5      |
| expertvillage  | 0KYGFawp14c  | 24659     | 6      |
| expertvillage  | j4DpuPvMLF4  | 22593     | 7      |
| expertvillage  | Msu4lZb2oeQ  | 18822     | 8      |
| expertvillage  | ZHZVj44rpjE  | 16304     | 9      |
| expertvillage  | foATQY3wovI  | 13576     | 10     |
| expertvillage  | -UnQ8rcBOQs  | 13450     | 11     |
| expertvillage  | crtNd46CDks  | 11639     | 12     |
| expertvillage  | D1leA0JKHhE  | 11553     | 13     |
| expertvillage  | NJu2oG1Wm98  | 11452     | 14     |
| expertvillage  | CapbXdyv4j4  | 10915     | 15     |
| expertvillage  | epr5erraEp4  | 10817     | 16     |
| expertvillage  | IyQoDgaLM7U  | 10597     | 17     |
| expertvillage  | tbZibBnusLQ  | 10402     | 18     |
| expertvillage  | _GnCHodc7mk  | 9422      | 19     |
| expertvillage  | hvEYlSlRitU  | 7123      | 20     |
| Ruchaneewan    | 5_T5Inddsuo  | 3132      | 1      |
| Ruchaneewan    | wje4lUtbYNU  | 1086      | 2      |
| Ruchaneewan    | i8rLbOUhAlM  | 549       | 3      |
| Ruchaneewan    | OwnEtde9_Co  | 453       | 4      |
| Ruchaneewan    | 5Zf0lbAdJP0  | 441       | 5      |
| Ruchaneewan    | wenI5MrYT20  | 426       | 6      |
| Ruchaneewan    | 3hzOiFP-5so  | 420       | 7      |
| Ruchaneewan    | Iq4e3SopjxQ  | 420       | 7      |
| Ruchaneewan    | JgyOlXjjuw0  | 418       | 9      |
| Ruchaneewan    | fGBVShTsuyo  | 395       | 10     |
| Ruchaneewan    | O3aoL70DlVc  | 389       | 11     |
| Ruchaneewan    | q4y2ZS5OQ88  | 344       | 12     |
| Ruchaneewan    | lyUJB2eMVVg  | 271       | 13     |
| Ruchaneewan    | _RF_3VhaQpw  | 242       | 14     |
| Ruchaneewan    | DDl2cjI-aJs  | 231       | 15     |
| Ruchaneewan    | xbYyjUdhtJw  | 227       | 16     |
| Ruchaneewan    | 4dkKeIUkN7E  | 226       | 17     |
| Ruchaneewan    | qCfuQA6N4K0  | 213       | 18     |
| Ruchaneewan    | TmYbGQaRcNM  | 209       | 19     |
| Ruchaneewan    | dOlfPsFSjw0  | 206       | 20     |
+----------------+--------------+-----------+--------+

-- 约定2: 取Top10中所有人上传的视频的前20
-- a. top10用户
SELECT uploader, videos
FROM gulivideo_user_orc
ORDER BY videos DESC 
LIMIT 10   ==>t1

-- b. 关联gulivideo_orc ,取到这10个用户上传的视频观看数前20
SELECT t2.uploader, t2.videoId, t2.views
FROM t1 JOIN gulivideo_orc  t2
ON t1.uploader =  t2.uploader
ORDER BY t2.views DESC 
LIMIT 20 

-- c. 组合
SELECT t2.uploader, t2.videoId, t2.views
FROM (SELECT uploader, videos
FROM gulivideo_user_orc
ORDER BY videos DESC 
LIMIT 10)t1 JOIN gulivideo_orc  t2
ON t1.uploader =  t2.uploader
ORDER BY t2.views DESC 
LIMIT 20 
+----------------+--------------+-----------+
|  t2.uploader   |  t2.videoid  | t2.views  |
+----------------+--------------+-----------+
| expertvillage  | -IxHBW0YpZw  | 39059     | 
| expertvillage  | BU-fT5XI_8I  | 29975     |
| expertvillage  | ADOcaBYbMl0  | 26270     |
| expertvillage  | yAqsULIDJFE  | 25511     |
| expertvillage  | vcm-t0TJXNg  | 25366     |
| expertvillage  | 0KYGFawp14c  | 24659     |
| expertvillage  | j4DpuPvMLF4  | 22593     |
| expertvillage  | Msu4lZb2oeQ  | 18822     |
| expertvillage  | ZHZVj44rpjE  | 16304     |
| expertvillage  | foATQY3wovI  | 13576     |
| expertvillage  | -UnQ8rcBOQs  | 13450     |
| expertvillage  | crtNd46CDks  | 11639     |
| expertvillage  | D1leA0JKHhE  | 11553     |
| expertvillage  | NJu2oG1Wm98  | 11452     |
| expertvillage  | CapbXdyv4j4  | 10915     |
| expertvillage  | epr5erraEp4  | 10817     |
| expertvillage  | IyQoDgaLM7U  | 10597     |
| expertvillage  | tbZibBnusLQ  | 10402     |
| expertvillage  | _GnCHodc7mk  | 9422      |
| expertvillage  | hvEYlSlRitU  | 7123      |
+----------------+--------------+-----------+


-- 约定3: 取所有人上传的视频的前20 
-- a. top10用户
SELECT uploader, videos
FROM gulivideo_user_orc
ORDER BY videos DESC 
LIMIT 10   ==>t1

-- b. 关联gulivideo_orc ,取到这10个用户上传的视频观看数前20
SELECT t2.uploader, t2.videoId, t2.views
FROM t1 JOIN gulivideo_orc  t2
ON t1.uploader =  t2.uploader
ORDER BY t2.views DESC 
LIMIT 20  ==>t3

-- c. 找到所有视频中观看数前20的
SELECT videoId
FROM gulivideo_orc  
ORDER BY views DESC 
LIMIT 20  ==> t4

-- d. 关联
SELECT t3.uploader ,t3.videoId, t3.views
FROM t3 JOIN t4
ON t3.videoId = t4.videoId

-- e.组合
SELECT t3.uploader ,t3.videoId, t3.views
FROM (SELECT t2.uploader, t2.videoId, t2.views
FROM (SELECT uploader, videos
FROM gulivideo_user_orc
ORDER BY videos DESC 
LIMIT 10)t1 JOIN gulivideo_orc  t2
ON t1.uploader =  t2.uploader
ORDER BY t2.views DESC 
LIMIT 20)t3 JOIN (SELECT videoId
FROM gulivideo_orc  
ORDER BY views DESC 
LIMIT 20 )t4
ON t3.videoId = t4.videoId

==========================================================================
课后练习:

课后练习一:
数据:
u01     2017/1/21       5
u02     2017/1/23       6
u03     2017/1/22       8
u04     2017/1/20       3
u01     2017/1/23       6
u01     2017/2/21       8
u02     2017/1/23       6
u01     2017/2/22       4

结果:
用户id	月份	小计	累积
u01	2017-01	11	11
u01	2017-02	12	23
u02	2017-01	12	12
u03	2017-01	8	8
u04	2017-01	3	3


a. 先处理日期格式

SELECT userId,  
DATE_FORMAT(regexp_replace(visitDate,"/","-"),'yyyy-MM') visitDate , visitCount
FROM ACTION    ==>t1
 

b. 根据用户id和月份分组，求小计
SELECT t1.userId, t1.visitDate, SUM(visitCount) xj
FROM t1 
GROUP BY userId,visitDate   ==>t2

组合:
SELECT t1.userId, t1.visitDate, SUM(visitCount) xj
FROM (SELECT userId,  
DATE_FORMAT(regexp_replace(visitDate,"/","-"),'yyyy-MM') visitDate , visitCount
FROM ACTION)t1 
GROUP BY userId,visitDate

+------------+---------------+-----+--+
| t1.userid  | t1.visitdate  | xj  |
+------------+---------------+-----+--+
| u01        | 2017-01       | 11  |
| u01        | 2017-02       | 12  |
| u02        | 2017-01       | 12  |
| u03        | 2017-01       | 8   |
| u04        | 2017-01       | 3   |
+------------+---------------+-----+--+


c. 开窗，通过userId分区,visitData排序, 求累计

SELECT t2.userId,t2.visitDate, t2.xj ,  
SUM(t2.xj) over(PARTITION BY t2.userId ORDER BY t2.visitDate) lj
FROM t2 


组合:
SELECT t2.userId,t2.visitDate, t2.xj ,  
SUM(t2.xj) over(PARTITION BY t2.userId ORDER BY t2.visitDate) lj
FROM (SELECT t1.userId, t1.visitDate, SUM(visitCount) xj
FROM (SELECT userId,  
DATE_FORMAT(regexp_replace(visitDate,"/","-"),'yyyy-MM') visitDate , visitCount
FROM ACTION)t1 
GROUP BY userId,visitDate)t2 

+------------+---------------+--------+-----+--+
| t2.userid  | t2.visitdate  | t2.xj  | lj  |
+------------+---------------+--------+-----+--+
| u01        | 2017-01       | 11     | 11  |
| u01        | 2017-02       | 12     | 23  |
| u02        | 2017-01       | 12     | 12  |
| u03        | 2017-01       | 8      | 8   |
| u04        | 2017-01       | 3      | 3   |
+------------+---------------+--------+-----+--+


====================================================

课后练习二:
+----------------+-------------+--+
| visit.user_id  | visit.shop  |
+----------------+-------------+--+
| u1             | a           |
| u2             | b           |
| u1             | b           |
| u1             | a           |
| u3             | c           |
| u4             | b           |
| u1             | a           |
| u2             | c           |
| u5             | b           |
| u4             | b           |
| u6             | c           |
| u2             | c           |
| u1             | b           |
| u2             | a           |
| u2             | a           |
| u3             | a           |
| u5             | a           |
| u5             | a           |
| u5             | a           |
+----------------+-------------+--+

需求一:  每个店铺的UV（访客数）

思路一:  按照店铺分组，通过count(DISTINCT) 求UV

SELECT shop ,COUNT(DISTINCT user_id) uv 
FROM visit
GROUP BY shop 

+-------+-----+--+
| shop  | uv  |
+-------+-----+--+
| a     | 4   |
| b     | 4   |
| c     | 3   |
+-------+-----+--+


思路二: 
a. 按照user_id 和shop分组 ，求出哪个用户访问过哪个店铺
SELECT  user_id, shop 
FROM visit
GROUP BY user_id, shop   ==>t1

+----------+-------+--+
| user_id  | shop  |
+----------+-------+--+
| u1       | a     |
| u1       | b     |
| u2       | a     |
| u2       | b     |
| u2       | c     |
| u3       | a     |
| u3       | c     |
| u4       | b     |
| u5       | a     |
| u5       | b     |
| u6       | c     |
+----------+-------+--+

b. 按照shop分组，求uv
SELECT t1.shop ,COUNT(t1.user_id) uv 
FROM t1
GROUP BY t1.shop 

组合:
SELECT t1.shop ,COUNT(t1.user_id) uv 
FROM (SELECT  user_id, shop 
FROM visit
GROUP BY user_id, shop)t1
GROUP BY t1.shop 

+----------+-----+--+
| t1.shop  | uv  |
+----------+-----+--+
| a        | 4   |
| b        | 4   |
| c        | 3   |
+----------+-----+--+


需求二:每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数

a. 通过user_id, shop分组，求每个店铺，每个访客访问的次数
SELECT user_id, shop ,COUNT(user_id) visit_count
FROM visit
GROUP BY user_id, shop   ==>t1

+----------+-------+--------------+--+
| user_id  | shop  | visit_count  |
+----------+-------+--------------+--+
| u1       | a     | 3            |
| u1       | b     | 2            |
| u2       | a     | 2            |
| u2       | b     | 1            |
| u2       | c     | 2            |
| u3       | a     | 1            |
| u3       | c     | 1            |
| u4       | b     | 2            |
| u5       | a     | 3            |
| u5       | b     | 1            |
| u6       | c     | 1            |
+----------+-------+--------------+--+

b.按照shop分区,访问次数排序,求排名

SELECT t1.user_id ,t1.shop ,t1.visit_count,
rank() over(distribute BY t1.shop sort BY t1.visit_count DESC ) rk
FROM t1   ==>t2

组合:
SELECT t1.user_id ,t1.shop ,t1.visit_count,
rank() over(distribute BY t1.shop sort BY t1.visit_count DESC ) rk
FROM (SELECT user_id, shop ,COUNT(user_id) visit_count
FROM visit
GROUP BY user_id, shop )t1  

+-------------+----------+-----------------+-----+--+
| t1.user_id  | t1.shop  | t1.visit_count  | rk  |
+-------------+----------+-----------------+-----+--+
| u5          | a        | 3               | 1   |
| u1          | a        | 3               | 1   |
| u2          | a        | 2               | 3   |
| u3          | a        | 1               | 4   |
| u4          | b        | 2               | 1   |
| u1          | b        | 2               | 1   |
| u5          | b        | 1               | 3   |
| u2          | b        | 1               | 3   |
| u2          | c        | 2               | 1   |
| u6          | c        | 1               | 2   |
| u3          | c        | 1               | 2   |
+-------------+----------+-----------------+-----+--+



c. 取top3
SELECT t2.user_id, t2.shop, t2.visit_count 
FROM t2 
WHERE t2.rk <=3

组合: 
SELECT t2.user_id, t2.shop, t2.visit_count 
FROM (SELECT t1.user_id ,t1.shop ,t1.visit_count,
rank() over(distribute BY t1.shop sort BY t1.visit_count DESC ) rk
FROM (SELECT user_id, shop ,COUNT(user_id) visit_count
FROM visit
GROUP BY user_id, shop )t1  )t2 
WHERE t2.rk <=3

+-------------+----------+-----------------+--+
| t2.user_id  | t2.shop  | t2.visit_count  |
+-------------+----------+-----------------+--+
| u5          | a        | 3               |
| u1          | a        | 3               |
| u2          | a        | 2               |
| u4          | b        | 2               |
| u1          | b        | 2               |
| u5          | b        | 1               |
| u2          | b        | 1               |
| u2          | c        | 2               |
| u6          | c        | 1               |
| u3          | c        | 1               |
+-------------+----------+-----------------+--+


=================================================

课后练习三: 

低碳(能量)流水表:
table_name：user_low_carbon

user_id    data_dt     low_carbon
用户        日期         减少碳排放（g）

蚂蚁森林植物换购表:
table_name:  plant_carbon

plant_id     plant_name     low_carbon
植物编号	植物名	    换购植物所需要的碳


-- 需求一:
问题：假设2017年1月1日开始记录低碳数据（user_low_carbon），
      假设2017年10月1日之前满足申领条件的用户都申领了一颗p004-胡杨，
      剩余的能量全部用来领取“p002-沙柳” 。
      统计在10月1日累计申领“p002-沙柳” 排名前10的用户信息；
      以及他比后一名多领了几颗沙柳。

-- a. 格式化日期
SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt ,low_carbon
FROM user_low_carbon  ==>t1

-- b. 计算2017年10月1号前 每个人的总能量
SELECT t1.user_id, SUM(t1.low_carbon)
FROM  t1
WHERE MONTH(t1.data_dt) < '10'
GROUP BY  t1.user_id 
ORDER BY  sum_low_carbon DESC 
LIMIT 11  ==>t2

-- 组合:
SELECT t1.user_id, SUM(t1.low_carbon) sum_low_carbon
FROM  (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt ,low_carbon
FROM user_low_carbon )t1
WHERE MONTH(t1.data_dt) < '10'
GROUP BY  t1.user_id 
ORDER BY  sum_low_carbon DESC 
LIMIT 11
+-------------+-----------------+
| t1.user_id  | sum_low_carbon  |
+-------------+-----------------+
| u_007       | 1470            |
| u_013       | 1430            |
| u_008       | 1240            |
| u_005       | 1100            |
| u_010       | 1080            |
| u_014       | 1060            |
| u_011       | 960             |
| u_009       | 930             |
| u_006       | 830             |
| u_002       | 659             |
| u_004       | 640             |
+-------------+-----------------+


-- c. 计算每个人申领多少颗沙柳

SELECT  t2.user_id ,t2.sum_low_carbon , 
FLOOR((t2.sum_low_carbon-(SELECT low_carbon FROM plant_carbon WHERE  plant_id = 'p004'))/(SELECT low_carbon FROM plant_carbon WHERE  plant_id = 'p002')) sl_sum
FROM t2   ==>t3

-- 组合:
SELECT  t2.user_id ,t2.sum_low_carbon , 
FLOOR((t2.sum_low_carbon-(SELECT low_carbon FROM plant_carbon WHERE  plant_id = 'p004'))/(SELECT low_carbon FROM plant_carbon WHERE  plant_id = 'p002')) sl_sum
FROM (SELECT t1.user_id, SUM(t1.low_carbon) sum_low_carbon
FROM  (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt ,low_carbon
FROM user_low_carbon )t1
WHERE MONTH(t1.data_dt) < '10'
GROUP BY  t1.user_id 
ORDER BY  sum_low_carbon DESC 
LIMIT 11)t2  
+-------------+--------------------+---------+
| t2.user_id  | t2.sum_low_carbon  | sl_sum  |
+-------------+--------------------+---------+
| u_007       | 1470               | 66      |
| u_013       | 1430               | 63      |
| u_008       | 1240               | 53      |
| u_005       | 1100               | 46      |
| u_010       | 1080               | 45      |
| u_014       | 1060               | 44      |
| u_011       | 960                | 39      |
| u_009       | 930                | 37      |
| u_006       | 830                | 32      |
| u_002       | 659                | 23      |
| u_004       | 640                | 22      |
+-------------+--------------------+---------+

-- d. 将下一条数据提取到当前行

SELECT t3.user_id, t3.sum_low_carbon, t3.sl_sum,
lead(t3.sl_sum,1,0) over(ORDER BY sl_sum DESC ) next_sl_sum
FROM t3  ==>t4

-- 组合:
SELECT t3.user_id, t3.sum_low_carbon, t3.sl_sum,
lead(t3.sl_sum,1,0) over(ORDER BY sl_sum DESC ) next_sl_sum
FROM (SELECT  t2.user_id ,t2.sum_low_carbon , 
FLOOR((t2.sum_low_carbon-(SELECT low_carbon FROM plant_carbon WHERE  plant_id = 'p004'))/(SELECT low_carbon FROM plant_carbon WHERE  plant_id = 'p002')) sl_sum
FROM (SELECT t1.user_id, SUM(t1.low_carbon) sum_low_carbon
FROM  (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt ,low_carbon
FROM user_low_carbon )t1
WHERE MONTH(t1.data_dt) < '10'
GROUP BY  t1.user_id 
ORDER BY  sum_low_carbon DESC 
LIMIT 11)t2)t3 

+-------------+--------------------+------------+--------------+
| t3.user_id  | t3.sum_low_carbon  | t3.sl_sum  | next_sl_sum  |
+-------------+--------------------+------------+--------------+
| u_007       | 1470               | 66         | 63           |
| u_013       | 1430               | 63         | 53           |
| u_008       | 1240               | 53         | 46           |
| u_005       | 1100               | 46         | 45           |
| u_010       | 1080               | 45         | 44           |
| u_014       | 1060               | 44         | 39           |
| u_011       | 960                | 39         | 37           |
| u_009       | 930                | 37         | 32           |
| u_006       | 830                | 32         | 23           |
| u_002       | 659                | 23         | 22           |
| u_004       | 640                | 22         | 0            |
+-------------+--------------------+------------+--------------+


-- e. 计算比下个人多领取多少颗沙柳
SELECT t4.user_id, t4.sum_low_carbon,t4.sl_sum ,t4.next_sl_sum, t4.sl_sum-t4.next_sl_sum  sl_diff
FROM t4 
LIMIT 10 ;

-- 组合:
SELECT t4.user_id, t4.sum_low_carbon,t4.sl_sum ,t4.next_sl_sum, t4.sl_sum-t4.next_sl_sum  sl_diff
FROM (SELECT t3.user_id, t3.sum_low_carbon, t3.sl_sum,
lead(t3.sl_sum,1,0) over(ORDER BY sl_sum DESC ) next_sl_sum
FROM (SELECT  t2.user_id ,t2.sum_low_carbon , 
FLOOR((t2.sum_low_carbon-(SELECT low_carbon FROM plant_carbon WHERE  plant_id = 'p004'))/(SELECT low_carbon FROM plant_carbon WHERE  plant_id = 'p002')) sl_sum
FROM (SELECT t1.user_id, SUM(t1.low_carbon) sum_low_carbon
FROM  (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt ,low_carbon
FROM user_low_carbon )t1
WHERE MONTH(t1.data_dt) < '10'
GROUP BY  t1.user_id 
ORDER BY  sum_low_carbon DESC 
LIMIT 11)t2)t3 )t4 
LIMIT 10 ;


+-------------+--------------------+------------+-----------------+----------+
| t4.user_id  | t4.sum_low_carbon  | t4.sl_sum  | t4.next_sl_sum  | sl_diff  |
+-------------+--------------------+------------+-----------------+----------+
| u_007       | 1470               | 66         | 63              | 3        |
| u_013       | 1430               | 63         | 53              | 10       |
| u_008       | 1240               | 53         | 46              | 7        |
| u_005       | 1100               | 46         | 45              | 1        |
| u_010       | 1080               | 45         | 44              | 1        |
| u_014       | 1060               | 44         | 39              | 5        |
| u_011       | 960                | 39         | 37              | 2        |
| u_009       | 930                | 37         | 32              | 5        |
| u_006       | 830                | 32         | 23              | 9        |
| u_002       | 659                | 23         | 22              | 1        |
+-------------+--------------------+------------+-----------------+----------+




-- 需求二:
问题: 查询user_low_carbon表中每日流水记录，条件为：
      用户在2017年，连续三天（或以上）的天数里，
      每天减少碳排放（low_carbon）都超过100g的用户低碳流水。
      需要查询返回满足以上条件的user_low_carbon表中的记录流水

-- a. 格式化日期
SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon   ==>t1

-- b. 将每个人每天超过总能量超过100g的数据查出来
SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100   ==>t2
-- 组合

SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 
+-------------+-------------+---------------------+
| t1.user_id  | t1.data_dt  | day_sum_low_carbon  |
+-------------+-------------+---------------------+
| u_001       | 2017-01-02  | 270                 |
| u_001       | 2017-01-06  | 135                 |
| u_002       | 2017-01-02  | 220                 |
| u_002       | 2017-01-03  | 110                 |
| u_002       | 2017-01-04  | 150                 |
| u_002       | 2017-01-05  | 101                 |
| u_003       | 2017-01-02  | 160                 |
| u_003       | 2017-01-03  | 160                 |
| u_003       | 2017-01-05  | 120                 |
| u_003       | 2017-01-07  | 120                 |
| u_004       | 2017-01-01  | 110                 |
| u_004       | 2017-01-03  | 120                 |
| u_004       | 2017-01-06  | 120                 |
| u_004       | 2017-01-07  | 130                 |
| u_005       | 2017-01-02  | 130                 |
| u_005       | 2017-01-03  | 180                 |
| u_005       | 2017-01-04  | 190                 |
| u_005       | 2017-01-06  | 280                 |
| u_005       | 2017-01-07  | 160                 |
| u_006       | 2017-01-02  | 180                 |
| u_006       | 2017-01-03  | 220                 |
| u_006       | 2017-01-07  | 290                 |
| u_007       | 2017-01-01  | 130                 |
| u_007       | 2017-01-02  | 360                 |
| u_007       | 2017-01-04  | 530                 |
| u_007       | 2017-01-06  | 230                 |
| u_007       | 2017-01-07  | 160                 |
| u_008       | 2017-01-01  | 160                 |
| u_008       | 2017-01-02  | 120                 |
| u_008       | 2017-01-04  | 260                 |
| u_008       | 2017-01-05  | 360                 |
| u_008       | 2017-01-06  | 160                 |
| u_008       | 2017-01-07  | 120                 |
| u_009       | 2017-01-02  | 140                 |
| u_009       | 2017-01-03  | 170                 |
| u_009       | 2017-01-04  | 270                 |
| u_009       | 2017-01-07  | 140                 |
| u_010       | 2017-01-02  | 180                 |
| u_010       | 2017-01-04  | 170                 |
| u_010       | 2017-01-05  | 180                 |
| u_010       | 2017-01-06  | 190                 |
| u_010       | 2017-01-07  | 180                 |
| u_011       | 2017-01-01  | 110                 |
| u_011       | 2017-01-02  | 200                 |
| u_011       | 2017-01-03  | 120                 |
| u_011       | 2017-01-04  | 100                 |
| u_011       | 2017-01-05  | 100                 |
| u_011       | 2017-01-06  | 100                 |
| u_011       | 2017-01-07  | 230                 |
| u_012       | 2017-01-02  | 130                 |
| u_013       | 2017-01-02  | 200                 |
| u_013       | 2017-01-03  | 150                 |
| u_013       | 2017-01-04  | 550                 |
| u_013       | 2017-01-05  | 350                 |
| u_014       | 2017-01-01  | 220                 |
| u_014       | 2017-01-02  | 140                 |
| u_014       | 2017-01-05  | 250                 |
| u_014       | 2017-01-06  | 120                 |
| u_014       | 2017-01-07  | 290                 |
| u_015       | 2017-01-07  | 140                 |
+-------------+-------------+---------------------+


-- c. 连续三天
-- 1. 对于一条数据(今天)来说, 如何计算连续三天：
      前天  昨天  今天   
      昨天  今天  明天
      今天  明天  后天   
-- 2. 真正满足连续:
      今天-昨天 =1 AND  今天-前天= 2 
      OR
      今天-昨天 =1 AND  今天-明天=-1
      OR
      今天-明天=-1 AND  今天-后天=-2     
      
  u_002       | 2017-01-02  | 220                 |
| u_002       | 2017-01-03  | 110                 |
| u_002       | 2017-01-04  | 150                 |
| u_002       | 2017-01-05  | 101                 |
      

-- d. 将 前天 ， 昨天 ，明天 ， 后天的数据提取到当前行
SELECT t2.user_id, t2.day_sum_low_carbon , t2.data_dt  jt,
lag(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) qt ,
lag(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) zt ,
lead(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) mt ,
lead(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) ht 
FROM t2  ==>t3

-- 组合
SELECT t2.user_id, t2.day_sum_low_carbon , t2.data_dt  jt,
lag(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) qt ,
lag(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) zt ,
lead(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) mt ,
lead(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) ht 
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2

+-------------+------------------------+-------------+-------------+-------------+-------------+-------------+
| t2.user_id  | t2.day_sum_low_carbon  |     jt      |     qt      |     zt      |     mt      |     ht      |
+-------------+------------------------+-------------+-------------+-------------+-------------+-------------+
| u_001       | 270                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-06  | 1970-01-01  |
| u_001       | 135                    | 2017-01-06  | 1970-01-01  | 2017-01-02  | 1970-01-01  | 1970-01-01  |
| u_002       | 220                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-03  | 2017-01-04  |
| u_002       | 110                    | 2017-01-03  | 1970-01-01  | 2017-01-02  | 2017-01-04  | 2017-01-05  |
| u_002       | 150                    | 2017-01-04  | 2017-01-02  | 2017-01-03  | 2017-01-05  | 1970-01-01  |
| u_002       | 101                    | 2017-01-05  | 2017-01-03  | 2017-01-04  | 1970-01-01  | 1970-01-01  |
| u_003       | 160                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-03  | 2017-01-05  |
| u_003       | 160                    | 2017-01-03  | 1970-01-01  | 2017-01-02  | 2017-01-05  | 2017-01-07  |
| u_003       | 120                    | 2017-01-05  | 2017-01-02  | 2017-01-03  | 2017-01-07  | 1970-01-01  |
| u_003       | 120                    | 2017-01-07  | 2017-01-03  | 2017-01-05  | 1970-01-01  | 1970-01-01  |
| u_004       | 110                    | 2017-01-01  | 1970-01-01  | 1970-01-01  | 2017-01-03  | 2017-01-06  |
| u_004       | 120                    | 2017-01-03  | 1970-01-01  | 2017-01-01  | 2017-01-06  | 2017-01-07  |
| u_004       | 120                    | 2017-01-06  | 2017-01-01  | 2017-01-03  | 2017-01-07  | 1970-01-01  |
| u_004       | 130                    | 2017-01-07  | 2017-01-03  | 2017-01-06  | 1970-01-01  | 1970-01-01  |
| u_005       | 130                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-03  | 2017-01-04  |
| u_005       | 180                    | 2017-01-03  | 1970-01-01  | 2017-01-02  | 2017-01-04  | 2017-01-06  |
| u_005       | 190                    | 2017-01-04  | 2017-01-02  | 2017-01-03  | 2017-01-06  | 2017-01-07  |
| u_005       | 280                    | 2017-01-06  | 2017-01-03  | 2017-01-04  | 2017-01-07  | 1970-01-01  |
| u_005       | 160                    | 2017-01-07  | 2017-01-04  | 2017-01-06  | 1970-01-01  | 1970-01-01  |
| u_006       | 180                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-03  | 2017-01-07  |
| u_006       | 220                    | 2017-01-03  | 1970-01-01  | 2017-01-02  | 2017-01-07  | 1970-01-01  |
| u_006       | 290                    | 2017-01-07  | 2017-01-02  | 2017-01-03  | 1970-01-01  | 1970-01-01  |
| u_007       | 130                    | 2017-01-01  | 1970-01-01  | 1970-01-01  | 2017-01-02  | 2017-01-04  |
| u_007       | 360                    | 2017-01-02  | 1970-01-01  | 2017-01-01  | 2017-01-04  | 2017-01-06  |
| u_007       | 530                    | 2017-01-04  | 2017-01-01  | 2017-01-02  | 2017-01-06  | 2017-01-07  |
| u_007       | 230                    | 2017-01-06  | 2017-01-02  | 2017-01-04  | 2017-01-07  | 1970-01-01  |
| u_007       | 160                    | 2017-01-07  | 2017-01-04  | 2017-01-06  | 1970-01-01  | 1970-01-01  |
| u_008       | 160                    | 2017-01-01  | 1970-01-01  | 1970-01-01  | 2017-01-02  | 2017-01-04  |
| u_008       | 120                    | 2017-01-02  | 1970-01-01  | 2017-01-01  | 2017-01-04  | 2017-01-05  |
| u_008       | 260                    | 2017-01-04  | 2017-01-01  | 2017-01-02  | 2017-01-05  | 2017-01-06  |
| u_008       | 360                    | 2017-01-05  | 2017-01-02  | 2017-01-04  | 2017-01-06  | 2017-01-07  |
| u_008       | 160                    | 2017-01-06  | 2017-01-04  | 2017-01-05  | 2017-01-07  | 1970-01-01  |
| u_008       | 120                    | 2017-01-07  | 2017-01-05  | 2017-01-06  | 1970-01-01  | 1970-01-01  |
| u_009       | 140                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-03  | 2017-01-04  |
| u_009       | 170                    | 2017-01-03  | 1970-01-01  | 2017-01-02  | 2017-01-04  | 2017-01-07  |
| u_009       | 270                    | 2017-01-04  | 2017-01-02  | 2017-01-03  | 2017-01-07  | 1970-01-01  |
| u_009       | 140                    | 2017-01-07  | 2017-01-03  | 2017-01-04  | 1970-01-01  | 1970-01-01  |
| u_010       | 180                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-04  | 2017-01-05  |
| u_010       | 170                    | 2017-01-04  | 1970-01-01  | 2017-01-02  | 2017-01-05  | 2017-01-06  |
| u_010       | 180                    | 2017-01-05  | 2017-01-02  | 2017-01-04  | 2017-01-06  | 2017-01-07  |
| u_010       | 190                    | 2017-01-06  | 2017-01-04  | 2017-01-05  | 2017-01-07  | 1970-01-01  |
| u_010       | 180                    | 2017-01-07  | 2017-01-05  | 2017-01-06  | 1970-01-01  | 1970-01-01  |
| u_011       | 110                    | 2017-01-01  | 1970-01-01  | 1970-01-01  | 2017-01-02  | 2017-01-03  |
| u_011       | 200                    | 2017-01-02  | 1970-01-01  | 2017-01-01  | 2017-01-03  | 2017-01-04  |
| u_011       | 120                    | 2017-01-03  | 2017-01-01  | 2017-01-02  | 2017-01-04  | 2017-01-05  |
| u_011       | 100                    | 2017-01-04  | 2017-01-02  | 2017-01-03  | 2017-01-05  | 2017-01-06  |
| u_011       | 100                    | 2017-01-05  | 2017-01-03  | 2017-01-04  | 2017-01-06  | 2017-01-07  |
| u_011       | 100                    | 2017-01-06  | 2017-01-04  | 2017-01-05  | 2017-01-07  | 1970-01-01  |
| u_011       | 230                    | 2017-01-07  | 2017-01-05  | 2017-01-06  | 1970-01-01  | 1970-01-01  |
| u_012       | 130                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 1970-01-01  | 1970-01-01  |
| u_013       | 200                    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-03  | 2017-01-04  |
| u_013       | 150                    | 2017-01-03  | 1970-01-01  | 2017-01-02  | 2017-01-04  | 2017-01-05  |
| u_013       | 550                    | 2017-01-04  | 2017-01-02  | 2017-01-03  | 2017-01-05  | 1970-01-01  |
| u_013       | 350                    | 2017-01-05  | 2017-01-03  | 2017-01-04  | 1970-01-01  | 1970-01-01  |
| u_014       | 220                    | 2017-01-01  | 1970-01-01  | 1970-01-01  | 2017-01-02  | 2017-01-05  |
| u_014       | 140                    | 2017-01-02  | 1970-01-01  | 2017-01-01  | 2017-01-05  | 2017-01-06  |
| u_014       | 250                    | 2017-01-05  | 2017-01-01  | 2017-01-02  | 2017-01-06  | 2017-01-07  |
| u_014       | 120                    | 2017-01-06  | 2017-01-02  | 2017-01-05  | 2017-01-07  | 1970-01-01  |
| u_014       | 290                    | 2017-01-07  | 2017-01-05  | 2017-01-06  | 1970-01-01  | 1970-01-01  |
| u_015       | 140                    | 2017-01-07  | 1970-01-01  | 1970-01-01  | 1970-01-01  | 1970-01-01  |
+-------------+------------------------+-------------+-------------+-------------+-------------+-------------+

-- e .求出日期的差

SELECT t3.user_id, t3.day_sum_low_carbon , t3.jt , 
DATEDIFF(t3.jt,t3.qt) jt_qt_diff,
DATEDIFF(t3.jt,t3.zt) jt_zt_diff,
DATEDIFF(t3.jt,t3.mt) jt_mt_diff,
DATEDIFF(t3.jt,t3.ht) jt_ht_diff
FROM t3   ==>t4

-- 组合:
SELECT t3.user_id, t3.day_sum_low_carbon , t3.jt , 
DATEDIFF(t3.jt,t3.qt) jt_qt_diff,
DATEDIFF(t3.jt,t3.zt) jt_zt_diff,
DATEDIFF(t3.jt,t3.mt) jt_mt_diff,
DATEDIFF(t3.jt,t3.ht) jt_ht_diff
FROM (SELECT t2.user_id, t2.day_sum_low_carbon , t2.data_dt  jt,
lag(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) qt ,
lag(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) zt ,
lead(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) mt ,
lead(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) ht 
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2)t3 

+-------------+------------------------+-------------+-------------+-------------+-------------+-------------+
| t3.user_id  | t3.day_sum_low_carbon  |    t3.jt    | jt_qt_diff  | jt_zt_diff  | jt_mt_diff  | jt_ht_diff  |
+-------------+------------------------+-------------+-------------+-------------+-------------+-------------+
| u_001       | 270                    | 2017-01-02  | 17168       | 17168       | -4          | 17168       |
| u_001       | 135                    | 2017-01-06  | 17172       | 4           | 17172       | 17172       |
| u_002       | 220                    | 2017-01-02  | 17168       | 17168       | -1          | -2          |
| u_002       | 110                    | 2017-01-03  | 17169       | 1           | -1          | -2          |
| u_002       | 150                    | 2017-01-04  | 2           | 1           | -1          | 17170       |
| u_002       | 101                    | 2017-01-05  | 2           | 1           | 17171       | 17171       |
| u_003       | 160                    | 2017-01-02  | 17168       | 17168       | -1          | -3          |
| u_003       | 160                    | 2017-01-03  | 17169       | 1           | -2          | -4          |
| u_003       | 120                    | 2017-01-05  | 3           | 2           | -2          | 17171       |
| u_003       | 120                    | 2017-01-07  | 4           | 2           | 17173       | 17173       |
| u_004       | 110                    | 2017-01-01  | 17167       | 17167       | -2          | -5          |
| u_004       | 120                    | 2017-01-03  | 17169       | 2           | -3          | -4          |
| u_004       | 120                    | 2017-01-06  | 5           | 3           | -1          | 17172       |
| u_004       | 130                    | 2017-01-07  | 4           | 1           | 17173       | 17173       |
| u_005       | 130                    | 2017-01-02  | 17168       | 17168       | -1          | -2          |
| u_005       | 180                    | 2017-01-03  | 17169       | 1           | -1          | -3          |
| u_005       | 190                    | 2017-01-04  | 2           | 1           | -2          | -3          |
| u_005       | 280                    | 2017-01-06  | 3           | 2           | -1          | 17172       |
| u_005       | 160                    | 2017-01-07  | 3           | 1           | 17173       | 17173       |
| u_006       | 180                    | 2017-01-02  | 17168       | 17168       | -1          | -5          |
| u_006       | 220                    | 2017-01-03  | 17169       | 1           | -4          | 17169       |
| u_006       | 290                    | 2017-01-07  | 5           | 4           | 17173       | 17173       |
| u_007       | 130                    | 2017-01-01  | 17167       | 17167       | -1          | -3          |
| u_007       | 360                    | 2017-01-02  | 17168       | 1           | -2          | -4          |
| u_007       | 530                    | 2017-01-04  | 3           | 2           | -2          | -3          |
| u_007       | 230                    | 2017-01-06  | 4           | 2           | -1          | 17172       |
| u_007       | 160                    | 2017-01-07  | 3           | 1           | 17173       | 17173       |
| u_008       | 160                    | 2017-01-01  | 17167       | 17167       | -1          | -3          |
| u_008       | 120                    | 2017-01-02  | 17168       | 1           | -2          | -3          |
| u_008       | 260                    | 2017-01-04  | 3           | 2           | -1          | -2          |
| u_008       | 360                    | 2017-01-05  | 3           | 1           | -1          | -2          |
| u_008       | 160                    | 2017-01-06  | 2           | 1           | -1          | 17172       |
| u_008       | 120                    | 2017-01-07  | 2           | 1           | 17173       | 17173       |
| u_009       | 140                    | 2017-01-02  | 17168       | 17168       | -1          | -2          |
| u_009       | 170                    | 2017-01-03  | 17169       | 1           | -1          | -4          |
| u_009       | 270                    | 2017-01-04  | 2           | 1           | -3          | 17170       |
| u_009       | 140                    | 2017-01-07  | 4           | 3           | 17173       | 17173       |
| u_010       | 180                    | 2017-01-02  | 17168       | 17168       | -2          | -3          |
| u_010       | 170                    | 2017-01-04  | 17170       | 2           | -1          | -2          |
| u_010       | 180                    | 2017-01-05  | 3           | 1           | -1          | -2          |
| u_010       | 190                    | 2017-01-06  | 2           | 1           | -1          | 17172       |
| u_010       | 180                    | 2017-01-07  | 2           | 1           | 17173       | 17173       |
| u_011       | 110                    | 2017-01-01  | 17167       | 17167       | -1          | -2          |
| u_011       | 200                    | 2017-01-02  | 17168       | 1           | -1          | -2          |
| u_011       | 120                    | 2017-01-03  | 2           | 1           | -1          | -2          |
| u_011       | 100                    | 2017-01-04  | 2           | 1           | -1          | -2          |
| u_011       | 100                    | 2017-01-05  | 2           | 1           | -1          | -2          |
| u_011       | 100                    | 2017-01-06  | 2           | 1           | -1          | 17172       |
| u_011       | 230                    | 2017-01-07  | 2           | 1           | 17173       | 17173       |
| u_012       | 130                    | 2017-01-02  | 17168       | 17168       | 17168       | 17168       |
| u_013       | 200                    | 2017-01-02  | 17168       | 17168       | -1          | -2          |
| u_013       | 150                    | 2017-01-03  | 17169       | 1           | -1          | -2          |
| u_013       | 550                    | 2017-01-04  | 2           | 1           | -1          | 17170       |
| u_013       | 350                    | 2017-01-05  | 2           | 1           | 17171       | 17171       |
| u_014       | 220                    | 2017-01-01  | 17167       | 17167       | -1          | -4          |
| u_014       | 140                    | 2017-01-02  | 17168       | 1           | -3          | -4          |
| u_014       | 250                    | 2017-01-05  | 4           | 3           | -1          | -2          |
| u_014       | 120                    | 2017-01-06  | 4           | 1           | -1          | 17172       |
| u_014       | 290                    | 2017-01-07  | 2           | 1           | 17173       | 17173       |
| u_015       | 140                    | 2017-01-07  | 17173       | 17173       | 17173       | 17173       |
+-------------+------------------------+-------------+-------------+-------------+-------------+-------------+


-- f. 求连续三天
SELECT t4.user_id, t4.day_sum_low_carbon , t4.jt
FROM t4 
WHERE 
jt_qt_diff = 2 AND jt_zt_diff = 1 
OR 
jt_zt_diff = 1 AND jt_mt_diff = -1
OR
jt_mt_diff = -1 AND jt_ht_diff = -2    ==>t5
 
-- 组合:
SELECT t4.user_id, t4.day_sum_low_carbon , t4.jt
FROM (SELECT t3.user_id, t3.day_sum_low_carbon , t3.jt , 
DATEDIFF(t3.jt,t3.qt) jt_qt_diff,
DATEDIFF(t3.jt,t3.zt) jt_zt_diff,
DATEDIFF(t3.jt,t3.mt) jt_mt_diff,
DATEDIFF(t3.jt,t3.ht) jt_ht_diff
FROM (SELECT t2.user_id, t2.day_sum_low_carbon , t2.data_dt  jt,
lag(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) qt ,
lag(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) zt ,
lead(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) mt ,
lead(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) ht 
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2)t3 )t4 
WHERE 
jt_qt_diff = 2 AND jt_zt_diff = 1 
OR 
jt_zt_diff = 1 AND jt_mt_diff = -1
OR
jt_mt_diff = -1 AND jt_ht_diff = -2

+-------------+------------------------+-------------+
| t4.user_id  | t4.day_sum_low_carbon  |    t4.jt    |
+-------------+------------------------+-------------+
| u_002       | 220                    | 2017-01-02  |
| u_002       | 110                    | 2017-01-03  |
| u_002       | 150                    | 2017-01-04  |
| u_002       | 101                    | 2017-01-05  |
| u_005       | 130                    | 2017-01-02  |
| u_005       | 180                    | 2017-01-03  |
| u_005       | 190                    | 2017-01-04  |
| u_008       | 260                    | 2017-01-04  |
| u_008       | 360                    | 2017-01-05  |
| u_008       | 160                    | 2017-01-06  |
| u_008       | 120                    | 2017-01-07  |
| u_009       | 140                    | 2017-01-02  |
| u_009       | 170                    | 2017-01-03  |
| u_009       | 270                    | 2017-01-04  |
| u_010       | 170                    | 2017-01-04  |
| u_010       | 180                    | 2017-01-05  |
| u_010       | 190                    | 2017-01-06  |
| u_010       | 180                    | 2017-01-07  |
| u_011       | 110                    | 2017-01-01  |
| u_011       | 200                    | 2017-01-02  |
| u_011       | 120                    | 2017-01-03  |
| u_011       | 100                    | 2017-01-04  |
| u_011       | 100                    | 2017-01-05  |
| u_011       | 100                    | 2017-01-06  |
| u_011       | 230                    | 2017-01-07  |
| u_013       | 200                    | 2017-01-02  |
| u_013       | 150                    | 2017-01-03  |
| u_013       | 550                    | 2017-01-04  |
| u_013       | 350                    | 2017-01-05  |
| u_014       | 250                    | 2017-01-05  |
| u_014       | 120                    | 2017-01-06  |
| u_014       | 290                    | 2017-01-07  |
+-------------+------------------------+-------------+


-- g.关联原表，取满足条件的记录流水
SELECT t5.user_id, t5.jt , t7.low_carbon
FROM t5  LEFT JOIN  t7  
ON t5.user_id = t7.user_id AND t5.jt = t7.data_dt

-- 组合:
SELECT t5.user_id, t5.jt , t7.low_carbon
FROM (SELECT t4.user_id, t4.day_sum_low_carbon , t4.jt
FROM (SELECT t3.user_id, t3.day_sum_low_carbon , t3.jt , 
DATEDIFF(t3.jt,t3.qt) jt_qt_diff,
DATEDIFF(t3.jt,t3.zt) jt_zt_diff,
DATEDIFF(t3.jt,t3.mt) jt_mt_diff,
DATEDIFF(t3.jt,t3.ht) jt_ht_diff
FROM (SELECT t2.user_id, t2.day_sum_low_carbon , t2.data_dt  jt,
lag(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) qt ,
lag(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) zt ,
lead(t2.data_dt,1,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) mt ,
lead(t2.data_dt,2,'1970-01-01') over(PARTITION BY t2.user_id  ORDER BY t2.data_dt) ht 
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2)t3 )t4 
WHERE 
jt_qt_diff = 2 AND jt_zt_diff = 1 
OR 
jt_zt_diff = 1 AND jt_mt_diff = -1
OR
jt_mt_diff = -1 AND jt_ht_diff = -2)t5  
LEFT JOIN  
(SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t7  
ON t5.user_id = t7.user_id AND t5.jt = t7.data_dt

+-------------+-------------+----------------+
| t5.user_id  |    t5.jt    | t7.low_carbon  |
+-------------+-------------+----------------+
| u_010       | 2017-01-06  | 190            |
| u_011       | 2017-01-02  | 100            |
| u_011       | 2017-01-02  | 100            |
| u_002       | 2017-01-02  | 150            |
| u_002       | 2017-01-02  | 70             |
| u_002       | 2017-01-05  | 101            |
| u_009       | 2017-01-02  | 70             |
| u_009       | 2017-01-02  | 70             |
| u_013       | 2017-01-04  | 550            |
| u_002       | 2017-01-04  | 150            |
| u_008       | 2017-01-06  | 160            |
| u_013       | 2017-01-02  | 150            |
| u_013       | 2017-01-02  | 50             |
| u_014       | 2017-01-07  | 270            |
| u_014       | 2017-01-07  | 20             |
| u_002       | 2017-01-03  | 30             |
| u_002       | 2017-01-03  | 80             |
| u_010       | 2017-01-05  | 90             |
| u_010       | 2017-01-05  | 90             |
| u_011       | 2017-01-04  | 100            |
| u_011       | 2017-01-05  | 100            |
| u_005       | 2017-01-03  | 180            |
| u_011       | 2017-01-01  | 110            |
| u_011       | 2017-01-06  | 100            |
| u_011       | 2017-01-07  | 130            |
| u_011       | 2017-01-07  | 100            |
| u_014       | 2017-01-05  | 250            |
| u_005       | 2017-01-02  | 50             |
| u_005       | 2017-01-02  | 80             |
| u_010       | 2017-01-04  | 90             |
| u_010       | 2017-01-04  | 80             |
| u_008       | 2017-01-04  | 260            |
| u_008       | 2017-01-05  | 360            |
| u_008       | 2017-01-07  | 60             |
| u_008       | 2017-01-07  | 60             |
| u_010       | 2017-01-07  | 90             |
| u_010       | 2017-01-07  | 90             |
| u_011       | 2017-01-03  | 120            |
| u_013       | 2017-01-03  | 150            |
| u_013       | 2017-01-05  | 350            |
| u_005       | 2017-01-04  | 180            |
| u_005       | 2017-01-04  | 10             |
| u_009       | 2017-01-04  | 270            |
| u_009       | 2017-01-03  | 170            |
| u_014       | 2017-01-06  | 120            |
+-------------+-------------+----------------+

======================================================================

-- 需求二扩展: 求连续n天超过100g的记录流水

-- a. 格式化日期
SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon   ==>t1

-- b. 将每个人每天超过总能量超过100g的数据查出来
SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100   ==>t2
-- 组合

SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 
+-------------+-------------+---------------------+
| t1.user_id  | t1.data_dt  | day_sum_low_carbon  |
+-------------+-------------+---------------------+
| u_001       | 2017-01-02  | 270                 |
| u_001       | 2017-01-06  | 135                 |
| u_002       | 2017-01-02  | 220                 |
| u_002       | 2017-01-03  | 110                 |
| u_002       | 2017-01-04  | 150                 |
| u_002       | 2017-01-05  | 101                 |
| u_003       | 2017-01-02  | 160                 |
| u_003       | 2017-01-03  | 160                 |
| u_003       | 2017-01-05  | 120                 |
| u_003       | 2017-01-07  | 120                 |
| u_004       | 2017-01-01  | 110                 |
| u_004       | 2017-01-03  | 120                 |
| u_004       | 2017-01-06  | 120                 |
| u_004       | 2017-01-07  | 130                 |
| u_005       | 2017-01-02  | 130                 |
| u_005       | 2017-01-03  | 180                 |
| u_005       | 2017-01-04  | 190                 |
| u_005       | 2017-01-06  | 280                 |
| u_005       | 2017-01-07  | 160                 |
| u_006       | 2017-01-02  | 180                 |
| u_006       | 2017-01-03  | 220                 |
| u_006       | 2017-01-07  | 290                 |
| u_007       | 2017-01-01  | 130                 |
| u_007       | 2017-01-02  | 360                 |
| u_007       | 2017-01-04  | 530                 |
| u_007       | 2017-01-06  | 230                 |
| u_007       | 2017-01-07  | 160                 |
| u_008       | 2017-01-01  | 160                 |
| u_008       | 2017-01-02  | 120                 |
| u_008       | 2017-01-04  | 260                 |
| u_008       | 2017-01-05  | 360                 |
| u_008       | 2017-01-06  | 160                 |
| u_008       | 2017-01-07  | 120                 |
| u_009       | 2017-01-02  | 140                 |
| u_009       | 2017-01-03  | 170                 |
| u_009       | 2017-01-04  | 270                 |
| u_009       | 2017-01-07  | 140                 |
| u_010       | 2017-01-02  | 180                 |
| u_010       | 2017-01-04  | 170                 |
| u_010       | 2017-01-05  | 180                 |
| u_010       | 2017-01-06  | 190                 |
| u_010       | 2017-01-07  | 180                 |
| u_011       | 2017-01-01  | 110                 |
| u_011       | 2017-01-02  | 200                 |
| u_011       | 2017-01-03  | 120                 |
| u_011       | 2017-01-04  | 100                 |
| u_011       | 2017-01-05  | 100                 |
| u_011       | 2017-01-06  | 100                 |
| u_011       | 2017-01-07  | 230                 |
| u_012       | 2017-01-02  | 130                 |
| u_013       | 2017-01-02  | 200                 |
| u_013       | 2017-01-03  | 150                 |
| u_013       | 2017-01-04  | 550                 |
| u_013       | 2017-01-05  | 350                 |
| u_014       | 2017-01-01  | 220                 |
| u_014       | 2017-01-02  | 140                 |
| u_014       | 2017-01-05  | 250                 |
| u_014       | 2017-01-06  | 120                 |
| u_014       | 2017-01-07  | 290                 |
| u_015       | 2017-01-07  | 140                 |
+-------------+-------------+---------------------+



-- c. 连续3天
  u_002       | 2017-01-02   1   2017-01-01
| u_002       | 2017-01-03   2   2017-01-01
| u_002       | 2017-01-04   3   2017-01-01
| u_002       | 2017-01-05   4   2017-01-01
| u_002       | 2017-01-07   5   2017-01-02
| u_002       | 2017-01-08   6   2017-01-02
| u_002         2017-01-09   7   2017-01-02

-- d. 按照人分区，日期排序， 求row_number()
SELECT t2.user_id, t2.data_dt, t2.day_sum_low_carbon ,
row_number() over(PARTITION BY t2.user_id ORDER BY t2.data_dt ) rn
FROM t2   ==>t3

-- 组合:
SELECT t2.user_id, t2.data_dt, t2.day_sum_low_carbon ,
row_number() over(PARTITION BY t2.user_id ORDER BY t2.data_dt ) rn
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2 

+-------------+-------------+------------------------+-----+
| t2.user_id  | t2.data_dt  | t2.day_sum_low_carbon  | rn  |
+-------------+-------------+------------------------+-----+
| u_001       | 2017-01-02  | 270                    | 1   |
| u_001       | 2017-01-06  | 135                    | 2   |
| u_002       | 2017-01-02  | 220                    | 1   |
| u_002       | 2017-01-03  | 110                    | 2   |
| u_002       | 2017-01-04  | 150                    | 3   |
| u_002       | 2017-01-05  | 101                    | 4   |
| u_003       | 2017-01-02  | 160                    | 1   |
| u_003       | 2017-01-03  | 160                    | 2   |
| u_003       | 2017-01-05  | 120                    | 3   |
| u_003       | 2017-01-07  | 120                    | 4   |
| u_004       | 2017-01-01  | 110                    | 1   |
| u_004       | 2017-01-03  | 120                    | 2   |
| u_004       | 2017-01-06  | 120                    | 3   |
| u_004       | 2017-01-07  | 130                    | 4   |
| u_005       | 2017-01-02  | 130                    | 1   |
| u_005       | 2017-01-03  | 180                    | 2   |
| u_005       | 2017-01-04  | 190                    | 3   |
| u_005       | 2017-01-06  | 280                    | 4   |
| u_005       | 2017-01-07  | 160                    | 5   |
| u_006       | 2017-01-02  | 180                    | 1   |
| u_006       | 2017-01-03  | 220                    | 2   |
| u_006       | 2017-01-07  | 290                    | 3   |
| u_007       | 2017-01-01  | 130                    | 1   |
| u_007       | 2017-01-02  | 360                    | 2   |
| u_007       | 2017-01-04  | 530                    | 3   |
| u_007       | 2017-01-06  | 230                    | 4   |
| u_007       | 2017-01-07  | 160                    | 5   |
| u_008       | 2017-01-01  | 160                    | 1   |
| u_008       | 2017-01-02  | 120                    | 2   |
| u_008       | 2017-01-04  | 260                    | 3   |
| u_008       | 2017-01-05  | 360                    | 4   |
| u_008       | 2017-01-06  | 160                    | 5   |
| u_008       | 2017-01-07  | 120                    | 6   |
| u_009       | 2017-01-02  | 140                    | 1   |
| u_009       | 2017-01-03  | 170                    | 2   |
| u_009       | 2017-01-04  | 270                    | 3   |
| u_009       | 2017-01-07  | 140                    | 4   |
| u_010       | 2017-01-02  | 180                    | 1   |
| u_010       | 2017-01-04  | 170                    | 2   |
| u_010       | 2017-01-05  | 180                    | 3   |
| u_010       | 2017-01-06  | 190                    | 4   |
| u_010       | 2017-01-07  | 180                    | 5   |
| u_011       | 2017-01-01  | 110                    | 1   |
| u_011       | 2017-01-02  | 200                    | 2   |
| u_011       | 2017-01-03  | 120                    | 3   |
| u_011       | 2017-01-04  | 100                    | 4   |
| u_011       | 2017-01-05  | 100                    | 5   |
| u_011       | 2017-01-06  | 100                    | 6   |
| u_011       | 2017-01-07  | 230                    | 7   |
| u_012       | 2017-01-02  | 130                    | 1   |
| u_013       | 2017-01-02  | 200                    | 1   |
| u_013       | 2017-01-03  | 150                    | 2   |
| u_013       | 2017-01-04  | 550                    | 3   |
| u_013       | 2017-01-05  | 350                    | 4   |
| u_014       | 2017-01-01  | 220                    | 1   |
| u_014       | 2017-01-02  | 140                    | 2   |
| u_014       | 2017-01-05  | 250                    | 3   |
| u_014       | 2017-01-06  | 120                    | 4   |
| u_014       | 2017-01-07  | 290                    | 5   |
| u_015       | 2017-01-07  | 140                    | 1   |
+-------------+-------------+------------------------+-----+

 
-- e. 求data_dt 和rn的差值
SELECT t3.user_id , t3.data_dt, t3.day_sum_low_carbon , t3.rn ,
DATE_SUB(t3.data_dt,t3.rn) data_dt_rn_diff
FROM t3   ==>t4

-- 组合
SELECT t3.user_id , t3.data_dt, t3.day_sum_low_carbon , t3.rn ,
DATE_SUB(t3.data_dt,t3.rn) data_dt_rn_diff
FROM (SELECT t2.user_id, t2.data_dt, t2.day_sum_low_carbon ,
row_number() over(PARTITION BY t2.user_id ORDER BY t2.data_dt ) rn
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2 )t3 

+-------------+-------------+------------------------+--------+-------------+
| t3.user_id  | t3.data_dt  | t3.day_sum_low_carbon  | t3.rn  |    data_dt_rn_diff     |
+-------------+-------------+------------------------+--------+-------------+
| u_001       | 2017-01-02  | 270                    | 1      | 2017-01-01  |
| u_001       | 2017-01-06  | 135                    | 2      | 2017-01-04  |
| u_002       | 2017-01-02  | 220                    | 1      | 2017-01-01  |
| u_002       | 2017-01-03  | 110                    | 2      | 2017-01-01  |
| u_002       | 2017-01-04  | 150                    | 3      | 2017-01-01  |
| u_002       | 2017-01-05  | 101                    | 4      | 2017-01-01  |
| u_003       | 2017-01-02  | 160                    | 1      | 2017-01-01  |
| u_003       | 2017-01-03  | 160                    | 2      | 2017-01-01  |
| u_003       | 2017-01-05  | 120                    | 3      | 2017-01-02  |
| u_003       | 2017-01-07  | 120                    | 4      | 2017-01-03  |
| u_004       | 2017-01-01  | 110                    | 1      | 2016-12-31  |
| u_004       | 2017-01-03  | 120                    | 2      | 2017-01-01  |
| u_004       | 2017-01-06  | 120                    | 3      | 2017-01-03  |
| u_004       | 2017-01-07  | 130                    | 4      | 2017-01-03  |
| u_005       | 2017-01-02  | 130                    | 1      | 2017-01-01  |
| u_005       | 2017-01-03  | 180                    | 2      | 2017-01-01  |
| u_005       | 2017-01-04  | 190                    | 3      | 2017-01-01  |
| u_005       | 2017-01-06  | 280                    | 4      | 2017-01-02  |
| u_005       | 2017-01-07  | 160                    | 5      | 2017-01-02  |
| u_006       | 2017-01-02  | 180                    | 1      | 2017-01-01  |
| u_006       | 2017-01-03  | 220                    | 2      | 2017-01-01  |
| u_006       | 2017-01-07  | 290                    | 3      | 2017-01-04  |
| u_007       | 2017-01-01  | 130                    | 1      | 2016-12-31  |
| u_007       | 2017-01-02  | 360                    | 2      | 2016-12-31  |
| u_007       | 2017-01-04  | 530                    | 3      | 2017-01-01  |
| u_007       | 2017-01-06  | 230                    | 4      | 2017-01-02  |
| u_007       | 2017-01-07  | 160                    | 5      | 2017-01-02  |
| u_008       | 2017-01-01  | 160                    | 1      | 2016-12-31  |
| u_008       | 2017-01-02  | 120                    | 2      | 2016-12-31  |
| u_008       | 2017-01-04  | 260                    | 3      | 2017-01-01  |
| u_008       | 2017-01-05  | 360                    | 4      | 2017-01-01  |
| u_008       | 2017-01-06  | 160                    | 5      | 2017-01-01  |
| u_008       | 2017-01-07  | 120                    | 6      | 2017-01-01  |
| u_009       | 2017-01-02  | 140                    | 1      | 2017-01-01  |
| u_009       | 2017-01-03  | 170                    | 2      | 2017-01-01  |
| u_009       | 2017-01-04  | 270                    | 3      | 2017-01-01  |
| u_009       | 2017-01-07  | 140                    | 4      | 2017-01-03  |
| u_010       | 2017-01-02  | 180                    | 1      | 2017-01-01  |
| u_010       | 2017-01-04  | 170                    | 2      | 2017-01-02  |
| u_010       | 2017-01-05  | 180                    | 3      | 2017-01-02  |
| u_010       | 2017-01-06  | 190                    | 4      | 2017-01-02  |
| u_010       | 2017-01-07  | 180                    | 5      | 2017-01-02  |
| u_011       | 2017-01-01  | 110                    | 1      | 2016-12-31  |
| u_011       | 2017-01-02  | 200                    | 2      | 2016-12-31  |
| u_011       | 2017-01-03  | 120                    | 3      | 2016-12-31  |
| u_011       | 2017-01-04  | 100                    | 4      | 2016-12-31  |
| u_011       | 2017-01-05  | 100                    | 5      | 2016-12-31  |
| u_011       | 2017-01-06  | 100                    | 6      | 2016-12-31  |
| u_011       | 2017-01-07  | 230                    | 7      | 2016-12-31  |
| u_012       | 2017-01-02  | 130                    | 1      | 2017-01-01  |
| u_013       | 2017-01-02  | 200                    | 1      | 2017-01-01  |
| u_013       | 2017-01-03  | 150                    | 2      | 2017-01-01  |
| u_013       | 2017-01-04  | 550                    | 3      | 2017-01-01  |
| u_013       | 2017-01-05  | 350                    | 4      | 2017-01-01  |
| u_014       | 2017-01-01  | 220                    | 1      | 2016-12-31  |
| u_014       | 2017-01-02  | 140                    | 2      | 2016-12-31  |
| u_014       | 2017-01-05  | 250                    | 3      | 2017-01-02  |
| u_014       | 2017-01-06  | 120                    | 4      | 2017-01-02  |
| u_014       | 2017-01-07  | 290                    | 5      | 2017-01-02  |
| u_015       | 2017-01-07  | 140                    | 1      | 2017-01-06  |
+-------------+-------------+------------------------+--------+-------------+


-- f. 通过人和data_dt_rn_diff 分区. 求统计

SELECT t4.user_id, t4.data_dt, t4.day_sum_low_carbon ,t4.rn , t4.data_dt_rn_diff,
COUNT(t4.data_dt_rn_diff) over(PARTITION BY t4.user_id ,data_dt_rn_diff) lx_days
FROM  t4  ==>t5

-- 组合:
SELECT t4.user_id, t4.data_dt, t4.day_sum_low_carbon ,t4.rn , t4.data_dt_rn_diff,
COUNT(t4.data_dt_rn_diff) over(PARTITION BY t4.user_id ,data_dt_rn_diff) lx_days
FROM  (SELECT t3.user_id , t3.data_dt, t3.day_sum_low_carbon , t3.rn ,
DATE_SUB(t3.data_dt,t3.rn) data_dt_rn_diff
FROM (SELECT t2.user_id, t2.data_dt, t2.day_sum_low_carbon ,
row_number() over(PARTITION BY t2.user_id ORDER BY t2.data_dt ) rn
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2 )t3 )t4

+-------------+-------------+------------------------+--------+---------------------+----------+
| t4.user_id  | t4.data_dt  | t4.day_sum_low_carbon  | t4.rn  | t4.data_dt_rn_diff  | lx_days  |
+-------------+-------------+------------------------+--------+---------------------+----------+
| u_001       | 2017-01-02  | 270                    | 1      | 2017-01-01          | 1        |
| u_001       | 2017-01-06  | 135                    | 2      | 2017-01-04          | 1        |
| u_002       | 2017-01-02  | 220                    | 1      | 2017-01-01          | 4        |
| u_002       | 2017-01-03  | 110                    | 2      | 2017-01-01          | 4        |
| u_002       | 2017-01-04  | 150                    | 3      | 2017-01-01          | 4        |
| u_002       | 2017-01-05  | 101                    | 4      | 2017-01-01          | 4        |
| u_003       | 2017-01-03  | 160                    | 2      | 2017-01-01          | 2        |
| u_003       | 2017-01-02  | 160                    | 1      | 2017-01-01          | 2        |
| u_003       | 2017-01-05  | 120                    | 3      | 2017-01-02          | 1        |
| u_003       | 2017-01-07  | 120                    | 4      | 2017-01-03          | 1        |
| u_004       | 2017-01-01  | 110                    | 1      | 2016-12-31          | 1        |
| u_004       | 2017-01-03  | 120                    | 2      | 2017-01-01          | 1        |
| u_004       | 2017-01-06  | 120                    | 3      | 2017-01-03          | 2        |
| u_004       | 2017-01-07  | 130                    | 4      | 2017-01-03          | 2        |
| u_005       | 2017-01-02  | 130                    | 1      | 2017-01-01          | 3        |
| u_005       | 2017-01-03  | 180                    | 2      | 2017-01-01          | 3        |
| u_005       | 2017-01-04  | 190                    | 3      | 2017-01-01          | 3        |
| u_005       | 2017-01-06  | 280                    | 4      | 2017-01-02          | 2        |
| u_005       | 2017-01-07  | 160                    | 5      | 2017-01-02          | 2        |
| u_006       | 2017-01-02  | 180                    | 1      | 2017-01-01          | 2        |
| u_006       | 2017-01-03  | 220                    | 2      | 2017-01-01          | 2        |
| u_006       | 2017-01-07  | 290                    | 3      | 2017-01-04          | 1        |
| u_007       | 2017-01-01  | 130                    | 1      | 2016-12-31          | 2        |
| u_007       | 2017-01-02  | 360                    | 2      | 2016-12-31          | 2        |
| u_007       | 2017-01-04  | 530                    | 3      | 2017-01-01          | 1        |
| u_007       | 2017-01-06  | 230                    | 4      | 2017-01-02          | 2        |
| u_007       | 2017-01-07  | 160                    | 5      | 2017-01-02          | 2        |
| u_008       | 2017-01-01  | 160                    | 1      | 2016-12-31          | 2        |
| u_008       | 2017-01-02  | 120                    | 2      | 2016-12-31          | 2        |
| u_008       | 2017-01-05  | 360                    | 4      | 2017-01-01          | 4        |
| u_008       | 2017-01-04  | 260                    | 3      | 2017-01-01          | 4        |
| u_008       | 2017-01-06  | 160                    | 5      | 2017-01-01          | 4        |
| u_008       | 2017-01-07  | 120                    | 6      | 2017-01-01          | 4        |
| u_009       | 2017-01-03  | 170                    | 2      | 2017-01-01          | 3        |
| u_009       | 2017-01-04  | 270                    | 3      | 2017-01-01          | 3        |
| u_009       | 2017-01-02  | 140                    | 1      | 2017-01-01          | 3        |
| u_009       | 2017-01-07  | 140                    | 4      | 2017-01-03          | 1        |
| u_010       | 2017-01-02  | 180                    | 1      | 2017-01-01          | 1        |
| u_010       | 2017-01-04  | 170                    | 2      | 2017-01-02          | 4        |
| u_010       | 2017-01-05  | 180                    | 3      | 2017-01-02          | 4        |
| u_010       | 2017-01-06  | 190                    | 4      | 2017-01-02          | 4        |
| u_010       | 2017-01-07  | 180                    | 5      | 2017-01-02          | 4        |
| u_011       | 2017-01-05  | 100                    | 5      | 2016-12-31          | 7        |
| u_011       | 2017-01-01  | 110                    | 1      | 2016-12-31          | 7        |
| u_011       | 2017-01-02  | 200                    | 2      | 2016-12-31          | 7        |
| u_011       | 2017-01-03  | 120                    | 3      | 2016-12-31          | 7        |
| u_011       | 2017-01-04  | 100                    | 4      | 2016-12-31          | 7        |
| u_011       | 2017-01-06  | 100                    | 6      | 2016-12-31          | 7        |
| u_011       | 2017-01-07  | 230                    | 7      | 2016-12-31          | 7        |
| u_012       | 2017-01-02  | 130                    | 1      | 2017-01-01          | 1        |
| u_013       | 2017-01-02  | 200                    | 1      | 2017-01-01          | 4        |
| u_013       | 2017-01-03  | 150                    | 2      | 2017-01-01          | 4        |
| u_013       | 2017-01-04  | 550                    | 3      | 2017-01-01          | 4        |
| u_013       | 2017-01-05  | 350                    | 4      | 2017-01-01          | 4        |
| u_014       | 2017-01-01  | 220                    | 1      | 2016-12-31          | 2        |
| u_014       | 2017-01-02  | 140                    | 2      | 2016-12-31          | 2        |
| u_014       | 2017-01-05  | 250                    | 3      | 2017-01-02          | 3        |
| u_014       | 2017-01-06  | 120                    | 4      | 2017-01-02          | 3        |
| u_014       | 2017-01-07  | 290                    | 5      | 2017-01-02          | 3        |
| u_015       | 2017-01-07  | 140                    | 1      | 2017-01-06          | 1        |
+-------------+-------------+------------------------+--------+---------------------+----------+


-- g. 连续n天
SELECT t5.user_id, t5.data_dt, t5.day_sum_low_carbon ,t5.rn , t5.data_dt_rn_diff,t5.lx_days
FROM t5 
WHERE t5.lx_days >= 3  ==>t6

-- 组合:
SELECT t5.user_id, t5.data_dt, t5.day_sum_low_carbon ,t5.rn , t5.data_dt_rn_diff,t5.lx_days
FROM (SELECT t4.user_id, t4.data_dt, t4.day_sum_low_carbon ,t4.rn , t4.data_dt_rn_diff,
COUNT(t4.data_dt_rn_diff) over(PARTITION BY t4.user_id ,data_dt_rn_diff) lx_days
FROM  (SELECT t3.user_id , t3.data_dt, t3.day_sum_low_carbon , t3.rn ,
DATE_SUB(t3.data_dt,t3.rn) data_dt_rn_diff
FROM (SELECT t2.user_id, t2.data_dt, t2.day_sum_low_carbon ,
row_number() over(PARTITION BY t2.user_id ORDER BY t2.data_dt ) rn
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2 )t3 )t4)t5 
WHERE t5.lx_days >= 3


-- h. 关联原表，求记录流水
SELECT t6.user_id ,t6.data_dt, t7.low_carbon
FROM t6  LEFT JOIN  t7
ON t6.user_id = t7.user_id 
AND t6.data_dt = t7.data_dt

-- 组合：

SELECT t6.user_id ,t6.data_dt, t7.low_carbon
FROM (SELECT t5.user_id, t5.data_dt, t5.day_sum_low_carbon ,t5.rn , t5.data_dt_rn_diff,t5.lx_days
FROM (SELECT t4.user_id, t4.data_dt, t4.day_sum_low_carbon ,t4.rn , t4.data_dt_rn_diff,
COUNT(t4.data_dt_rn_diff) over(PARTITION BY t4.user_id ,data_dt_rn_diff) lx_days
FROM  (SELECT t3.user_id , t3.data_dt, t3.day_sum_low_carbon , t3.rn ,
DATE_SUB(t3.data_dt,t3.rn) data_dt_rn_diff
FROM (SELECT t2.user_id, t2.data_dt, t2.day_sum_low_carbon ,
row_number() over(PARTITION BY t2.user_id ORDER BY t2.data_dt ) rn
FROM (SELECT t1.user_id, t1.data_dt, SUM(t1.low_carbon) day_sum_low_carbon
FROM (SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t1 
WHERE YEAR(data_dt) = '2017'
GROUP BY t1.user_id , t1.data_dt
HAVING day_sum_low_carbon >=100 )t2 )t3 )t4)t5 
WHERE t5.lx_days >= 3)t6  
LEFT JOIN  
(SELECT user_id, DATE_FORMAT(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt , low_carbon
FROM user_low_carbon)t7
ON t6.user_id = t7.user_id 
AND t6.data_dt = t7.data_dt

+-------------+-------------+----------------+
| t6.user_id  | t6.data_dt  | t7.low_carbon  |
+-------------+-------------+----------------+
| u_010       | 2017-01-06  | 190            |
| u_011       | 2017-01-02  | 100            |
| u_011       | 2017-01-02  | 100            |
| u_002       | 2017-01-02  | 150            |
| u_002       | 2017-01-02  | 70             |
| u_002       | 2017-01-05  | 101            |
| u_009       | 2017-01-02  | 70             |
| u_009       | 2017-01-02  | 70             |
| u_013       | 2017-01-04  | 550            |
| u_002       | 2017-01-04  | 150            |
| u_008       | 2017-01-06  | 160            |
| u_013       | 2017-01-02  | 150            |
| u_013       | 2017-01-02  | 50             |
| u_014       | 2017-01-07  | 270            |
| u_014       | 2017-01-07  | 20             |
| u_002       | 2017-01-03  | 30             |
| u_002       | 2017-01-03  | 80             |
| u_010       | 2017-01-05  | 90             |
| u_010       | 2017-01-05  | 90             |
| u_011       | 2017-01-05  | 100            |
| u_011       | 2017-01-04  | 100            |
| u_005       | 2017-01-03  | 180            |
| u_011       | 2017-01-01  | 110            |
| u_011       | 2017-01-06  | 100            |
| u_011       | 2017-01-07  | 130            |
| u_011       | 2017-01-07  | 100            |
| u_014       | 2017-01-05  | 250            |
| u_005       | 2017-01-02  | 50             |
| u_005       | 2017-01-02  | 80             |
| u_010       | 2017-01-04  | 90             |
| u_010       | 2017-01-04  | 80             |
| u_008       | 2017-01-05  | 360            |
| u_008       | 2017-01-04  | 260            |
| u_008       | 2017-01-07  | 60             |
| u_008       | 2017-01-07  | 60             |
| u_010       | 2017-01-07  | 90             |
| u_010       | 2017-01-07  | 90             |
| u_011       | 2017-01-03  | 120            |
| u_013       | 2017-01-03  | 150            |
| u_013       | 2017-01-05  | 350            |
| u_005       | 2017-01-04  | 180            |
| u_005       | 2017-01-04  | 10             |
| u_009       | 2017-01-04  | 270            |
| u_009       | 2017-01-03  | 170            |
| u_014       | 2017-01-06  | 120            |
+-------------+-------------+----------------+


