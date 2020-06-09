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























-- 统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数
-- 统计视频观看数Top50所关联视频的所属类别Rank
-- 统计每个类别中的视频热度Top10,以Music为例
-- 统计每个类别视频观看数Top10
-- 统计上传视频最多的用户Top10以及他们上传的视频观看次数在前20的视频 















