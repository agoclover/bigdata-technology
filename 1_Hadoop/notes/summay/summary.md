# Hadoop

## Hadoop的组成

**hadoop 1.x**

- HDFS: 存
- MapReduce: 算 + 资源调度(内存, CPU, 磁盘, 网络带宽...)

**hadoop 2.x/3.x**  

- HDFS: 存
- MapReduce: 算
- Yarn: 资源调度



## HDFS的架构

### HDFS

Hadoop分布式文件系统, 文件系统是用于对文件进行存储和管理.

分布式可以理解为由多台机器共同构成一个完整的文件系统.

### NameNode(nn)

**描述**

负责管理HDFS中所有文件的元数据信息.

**元数据**

用于描述真实数据的数据就是所谓的元数据.

**举例:** 

一个真实的数据文件  `a.txt`. 

它的元数据为 `文件名 文件大小 文件权限  文件的目录结构  文件对应的块  在哪个dn存`.

**注意**

要想找到 HDFS 的真实数据必须通过 NameNode 所维护的元数据才能定位到DataNode中存储的真实数据    .             

### DataNode(dn)

**描述**

负责管理HDFS的所有的真实文件数据.

### SecondaryNameNode(2nn)

**描述**

辅助NameNode工作. 分担NameNode一些工作, 减轻NameNode的压力.

**注意**

1. 2nn 不是 nn的热备.顶多算nn的秘书.

2. 在一个集群中(非高可用集群): NN(1个)   DN(多个)   2NN(1个)



## Yarn的架构

### Yarn

资源调度和管理的框架. 管理和调度的资源就是整个Hadoop集群的资.

### ResourceManager(RM)

是Yarn的大哥. 负责管理和调度整个集群的资源. 负责处理客户端的请求.

负责为Job启动ApplicationMaster.

### NodeManager(NM)

是每台机器资源的管理者. 实际上只是将本机器的资源对ResourceManager做一个汇报.

对于资源的分配必须听从ResourceManager的指令.

### ApplicationMaster(AM)

对应每一个Job(MapReduce程序).

负责为Job向ResourceMananger去申请资源，申请到资源以后, 负责告诉NodeManager去运行对应的任务. 并且负责监控任务的运行状态和任务的容错.

### Container

对多维度资源的封装. 方便管理资源及防止资源被侵占.



## 简单模拟一个任务的提交过程和资源调度过程

 首先有一个Job (MapReduce程序): 包含 2 个MapTask  和 1 个ReduceTask

 1. 客户端提交 Job 到 ResourceManager.
  2. ResourceManager 为 Job 启动 ApplicationMaster, ApplicationMaster 的运行也需要资源, 因此 ApplicationMaster; 启动起来以后就会有一个 Container封装 ApplicationMaster 运行所用的资源. 因为资源都是在NodeManager上, 所以 ApplicationMaster 是运行在某一个 NodeManager 上.
  3. ApplicationMaster 会根据 Job 的情况向 ResourceManager 申请资源来运行每个 Task, 当前 Job 总共有 3 个Task. 每个Task都是单独运行, 因此需要申请 3 份资源, 也就意味着又有 3 个 Container 运行.
 4. 所有的资源的分配都是 ResourceManager 下达指令给 NodeManager 进行分配的.
  5. ApplicationMaster 为 Job 成功申请到资源以后, 会告诉 NodeManager 去运行对应的 Task; 每个 Task 可能运行到不同的机器, 也有可能多个 Task 运行到同一个机器,  要看当时集群的资源情况.
 6. 当 Job 的每个 Task 都开始运行, ApplicationMaster 负责监控整个 Job 的状态. 要负责容错相关的事情.
 7. 当 Job 的每个 Task 都执行成功后, 意味着 Job 运行完成, 此时 ApplicationMaster 会找 ResourceMananger 申请注销自己, 所有为当前 Job 申请的资源得到释放. 
    

## MapReduce的架构

### Map阶段 (MapTask)

负责将数据分到多台机器中，进行并行计算

### Reduce阶段 (ReduceTask)

负责将多台机器在map阶段中计算出来的数据，进行整体的汇总.



# 集群

## 简单操作

```shell
1)
在HDFS创建一个 /user/atguigu/input目录
hdfs dfs -mkdir -p /user/atguigu/input

2) 
将hadoop安装目录下的wcinput/wc.input 上传到 /user/atguigu/input 目录下
hdfs dfs -put wcinput/wc.input  /user/atguigu/input

3)
如何在HDFS查看具体存储的文件
DataNode存储文件的目录: /opt/module/hadoop-3.1.3/data/data/current/BP-1511121044-192.168.202.102-1589531664713/current/finalized/subdir0/subdir0

实际存储文件以块为单位进行存储,例如: blk_1073741825

4)
执行一个wordcount程序
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.3.jar wordcount /user/atguigu/input /user/atguigu/output

5)
查看hdfs上面执行后的结果
hdfs dfs -cat /user/atguigu/output/part-r-00000
```



## 配置文件

hadoop默认的配置文件

```
core-dafault.xml
hdfs-default.xml
yarn-default.xml
mapred-default.xml
```

用户自定义配置文件

```
core-site.xml
hdfs-site.xml
yarn-site.xml
mapred-site.xml
```

优先级

```
xxx-site.xml  >  xxx-default.xml
hadoop 启动的时候会先加载 xxx-default.xml, 再加载xxx-site.xml, 配置到 xxx-site.xml 中的配置会最终覆盖 xxx-default.xml 中的配置.
```



## 端口

9820  NameNode内部通信端口

9870  NameNode web端访问端口

9869  2NN 内部通信端口

9868  2NN web端访问端口

8088  ResourceManager web端访问端口

8032  ResourceManager 内部通信地址



## 格式化NameNode

格式化 NameNode 一般只在刚配置好集群, 第一次启动集群前进行格式化. 后续正常使用集群的情况下, 不需要重复格式化.

**重新格式化遇到的问题:**

- 格式化的时候需要我们进行确认操作

- 格式化完成后, Namenode能够启动, datanode启动不了(起来后又掉了)

**问题的原因:**

NameNode重新格式化后, 会生成新的集群id, 但是 Datanode 还记录原先的集群id, 此时Namenode 和 Datanode 的集群 id 不一致, 就会导致datanode起不来.

**如何解决:**

再重新格式化之前, 删除所有节点的hadoop安装目录下的 data目录(必须) 和  logs目录



## 日志

hadoop 日志在哪里:   hadoop的日志在 hadoop 的安装目录下的 logs 目录
查看方式:  `tail -n 100`  日志文件



# NN 与 2NN

### NameNode作用

最主要负责的事情就是对整个HDFS中数据的元数据进行管理.

### NameNode管理的元数据存在哪里?

内存+磁盘.

考虑数据的安全性或者是可靠性，元数据存到磁盘比较安全. 如果数据维护到磁盘,数据的安全可以保障。但是带来的问题是访问效率低. 因为修改元数据是在磁盘进行修改的。因此效率低. 

考虑到效率的问题，元数据存储到内存效率高. 带来的问题是数据不安全.因为内存的数据容易丢失，比如服务器掉电或者其他故障等...

结论: 内存和磁盘都存.

### 工作原理

如何在实现高效操作元数据的情况下, 还能实现内存+磁盘的维护方案?

HDFS 通过 fsimage(镜像文件) + edits(编辑日志)的方案来解决问题. 

镜像文件: 某个时刻对NN内存中元数据的一个快照. 镜像文件 <= NN内存中数据

编辑日志: 记录对HDFS的改操作.只做追加操作，因此效率高.

如果一直往edits文件中追加内容，改文件会变的特别大， 且会越来越大, 因此需要隔一段时间或者合适的时机进行 fsimage + edits文件的合并工作, 从而生成新的fsimage.

新的 fsimage = 旧的fsimage + edits.

将fsimage 和edits的合并工作交给2nn完成. 大概的过程就是将nn机器对应的磁盘上的 fsimage 和 edits文件拉取到2nn的机器中，在2nn的机器中将fsimage + edits都读取到内存中进行合并，然后生成新的fsimage ，再推送到nn机器中的磁盘上，nn中旧的fsimage会保留，新的fsimage对应的是正在使用.

### 具体的工作细节

1. NN启动时，需要自己先将磁盘的fsimage_current + edits001_progress 文件进行一次合并. 在内存中构建好元数据信息.
2. 当对HDFS进行改操作， 首先会在edits001_progress(正在使用的编辑日志)记录对应的操作，
   然后对NN内存中的元数据进行相应的修改.
3. 2NN会每隔1个小时或者当 edits001_progress中已经记录了100万次的操作后，开始进行fsimage_current 和edits001_progress的合并
4. NN会将edits_progress进行所谓的滚动，说白了就是该文件不能再进行写入操作，会生成另外一个编辑日志文件用于记录后续的写操作. 滚动正在使用的编辑日志: edits001_progress --> edits001新的编辑日志: edits002_progress.
5. 2NN 将NN中的fsimage_current 和 edits001 拷贝过来。加载到内存中进行合并. 合并完成后会生成新的fsimage_new.
6. 2NN 将fsimage_new 推送到NN中， fsiamge_current --> fsimage_old 会保留， 
   fsimage_new -->fsiamge_current .
   相当于会保留一份新的fsimage 和一份旧的fsiamge.
7. 总结: NN内存中的数据 = 磁盘上正在使用的fsimage + 正在使用的edits文件.

### FsImage 和 Edits

**Edits** 记录各种操作信息.

**FsImage** 记录元数据.

**HDFS的目录结构怎么维护的？**

在fsimage中通过 <inode>进行维护:

```
<inode><id>16386</id><type>DIRECTORY</type><name>user</name>....</inode>
<inode><id>16387</id><type>DIRECTORY</type><name>atguigu</name>...</inode>
```

目录与目录的上下级关系通过:

```
<INodeDirectorySection>
<directory><parent>16386</parent><child>16387</child></directory>
</INodeDirectorySection>
```

**对于文件的元数据信息来说, fsimage 中看不到块维护到哪些dn节点**

每个文件的块在哪些dn节点维护，是dn主动上报给nn的， 因此在nn的内存中是有记录的。 但是在磁盘的fsimage中是没有记录的.



# MR

**MapReduce 的核心思想**

先分(map)后合(reduce)

**MapReduce核心思想初认识**

1. 首先待计算的数据会先生成切片(逻辑上对数据进行划分) , 生成的切片个数对应着要启动多少个MapTask进行Map阶段的计算.
2. 多个MapTask是并行运行的，互不相干. 
3. 在每个MapTask中对数据的处理要考虑到很多细节, 是否有分区， 如何排序， 数据如何写磁盘等..
4. 多个MapTask计算完成后，每个MapTask都会有输出的数据
5. 会根据分区的个数决定启动多少个ReduceTask(逻辑上来说), 实际上是 启动多少个ReduceTask就会生成多少个分区.
6. 每个ReduceTask会到每个MapTask中拷贝自己所要处理的数据,说白了就是对应的分区的数据.
7. 每个ReduceTask最终也会输出最后的结果.
8. 待说明的问题:  
         a. 数据如何切片
     b. MapTask如何工作
     c. 中间过程的排序
     d. 数据如何分区
     e. ReduceTask如何工作
     f. MapTask和ReduceTask如何衔接...
     g. 。。。。。。。。