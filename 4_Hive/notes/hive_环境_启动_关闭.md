# HA

```
1. 开启 zookeeper: zk start
2. 开启 hdfs: start-dfs.sh
3. 开启 yarn: start-yarn.sh
```



# Hive

hive 环境

```
1. JDK
2. mysqld
3. Zookeeper
4. Hadoop-HA
5. HiveServices: metastore 和 hiveserver2
6. beenline 或 hive
```

启动

```
1. 开启 mysqld: sudo systemctl start mysqld
2. 开启 zookeeper: zk start
3. 开启 hdfs: start-dfs.sh
4. 开启 yarn: start-yarn.sh
5. 开启 hive 的 metastore 和 hiveserver2 这两个服务: hiveservices start
6. 启动 hive 的 beeline 或 hive 客户端:
	beeline -u jdbs:hive2://hadoop102:10000 -n atguigu
	hive
```

关闭

```
1. 退出 hive 的 beeline 或 hive 客户端: !quit / quit
2. 关闭 hive 的 metastore 和 hiveserver2 这两个服务: hiveservices stop
3. 关闭 yarn: stop-yarn.sh
4. 关闭 hdfs: stop-dfs.sh
5. 关闭 zookeeper: zk stop
6. 关闭 mysqld: sudo systemctl stop mysqld
```

