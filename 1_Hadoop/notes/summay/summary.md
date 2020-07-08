# Hadoop

## Hadoop的组成

**hadoop 1.x**:  HDFS: 存;	MapReduce: 算 + 资源调度(内存, CPU, 磁盘, 网络带宽...)

**hadoop 2.x/3.x**:  HDFS: 存;	MapReduce: 算;	Yarn: 资源调度



## HDFS的架构

### HDFS

Hadoop分布式文件系统, 文件系统是用于对文件进行存储和管理.

分布式可以理解为由多台机器共同构成一个完整的文件系统.

### NameNode(nn)

**描述**:	负责管理HDFS中所有文件的元数据信息.

**元数据**:	用于描述真实数据的数据就是所谓的元数据.

**举例**: 	一个真实的数据文件  `a.txt`.  它的元数据为 `文件名 文件大小 文件权限  文件的目录结构  文件对应的块  在哪个dn存`.

**注意**: 	要想找到 HDFS 的真实数据必须通过 NameNode 所维护的元数据才能定位到DataNode中存储的真实数据.             

### DataNode(dn)

**描述**:	负责管理 HDFS 的所有的真实文件数据.

### SecondaryNameNode(2nn)

**描述**:	辅助NameNode工作. 分担NameNode一些工作, 减轻NameNode的压力.

**注意**:

1. 2nn 不是 nn 的热备, 顶多算 nn 的秘书.

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



# Cluster

## HDFS 操作

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

hadoop 默认的配置文件:

```
core-dafault.xml
hdfs-default.xml
yarn-default.xml
mapred-default.xml
```

用户自定义配置文件:

```
core-site.xml
hdfs-site.xml
yarn-site.xml
mapred-site.xml
```

优先级:

```
xxx-site.xml  >  xxx-default.xml
hadoop 启动的时候会先加载 xxx-default.xml, 再加载xxx-site.xml, 配置到 xxx-site.xml 中的配置会最终覆盖 xxx-default.xml 中的配置.
```



## 端口

**9820**  NameNode内部通信端口

**9870**  NameNode web端访问端口

**9869**  2NN 内部通信端口

**9868**  2NN web端访问端口

**8088**  ResourceManager web端访问端口

**8032**  ResourceManager 内部通信地址



## 格式化NameNode

格式化 NameNode 一般只在刚配置好集群, 第一次启动集群前进行格式化. 后续正常使用集群的情况下, 不需要重复格式化.

**重新格式化遇到的问题:**

- 格式化的时候需要我们进行确认操作

- 格式化完成后, Namenode能够启动, datanode启动不了(起来后又掉了)

**问题的原因:**

NameNode 重新格式化后, 会生成新的集群id, 但是 Datanode 还记录原先的集群id, 此时Namenode 和 Datanode 的集群 id 不一致, 就会导致 datanode 起不来.

**如何解决:**

再重新格式化之前, 删除所有节点的 hadoop 安装目录下的 data目录(必须) 和  logs 目录.



## 日志

hadoop 日志在哪里:   hadoop的日志在 hadoop 的安装目录下的 logs 目录

查看方式:  `tail -n 100`  日志文件



# NN 与 2NN

### NameNode作用

最主要负责的事情就是对整个 HDFS 中数据的元数据进行管理.

### NameNode管理的元数据存在哪里?

考虑数据的安全性或者是可靠性, 元数据存到磁盘比较安全. 如果数据维护到磁盘, 数据的安全可以保障, 但是带来的问题是访问效率低. 因为修改元数据是在磁盘进行修改的, 因此效率低. 

考虑到效率的问题, 元数据存储到内存效率高. 带来的问题是数据不安全. 因为内存的数据容易丢失, 比如服务器掉电或者其他故障等.

结论: 内存和磁盘都存.

### 工作原理

如何在实现高效操作元数据的情况下, 还能实现内存 + 磁盘的维护方案?

HDFS 通过 fsimage(镜像文件) + edits(编辑日志) 的方案来解决问题. 

镜像文件: 某个时刻对 NN 内存中元数据的一个快照. 镜像文件 <= NN内存中数据

编辑日志: 记录对 HDFS 的改操作. 只做追加操作, 因此效率高.

如果一直往 edits 文件中追加内容, 改文件会变的特别大, 且会越来越大, 因此需要隔一段时间或者合适的时机进行 fsimage + edits 文件的合并工作, 从而生成新的 fsimage.

新的 fsimage = 旧的 fsimage + edits.

将 fsimage 和 edits 的合并工作交给 2nn 完成. 大概的过程就是将 nn 机器对应的磁盘上的 fsimage 和 edits 文件拉取到 2nn 的机器中, 在 2nn 的机器中将 fsimage + edits 都读取到内存中进行合并, 然后生成新的 fsimage, 再推送到 nn 机器中的磁盘上, nn 中旧的 fsimage 会保留, 新的 fsimage 对应的是正在使用.

### 具体的工作细节

1. NN 启动时, 需要自己先将磁盘的 fsimage_current + edits001_progress 文件进行一次合并. 在内存中构建好元数据信息.
2. 当对 HDFS 进行改操作, 首先会在 edits001_progress (正在使用的编辑日志) 记录对应的操作, 然后对 NN 内存中的元数据进行相应的修改.
3. 2NN 会每隔 1 个小时或者当 edits001_progress 中已经记录了 100 万次的操作后, 开始进行 fsimage_current 和 edits001_progress 的合并.
4. NN 会将 edits_progress 进行所谓的滚动, 说白了就是该文件不能再进行写入操作, 会生成另外一个编辑日志文件用于记录后续的写操作. 滚动正在使用的编辑日志: edits001_progress --> edits001. 新的编辑日志: edits002_progress.
5. 2NN 将 NN 中的 fsimage_current 和 edits001 拷贝过来. 加载到内存中进行合并. 合并完成后会生成新的fsimage_new.
6. 2NN 将 fsimage_new 推送到 NN 中, fsiamge_current --> fsimage_old 会保留,  fsimage_new --> fsiamge_current . 相当于会保留一份新的 fsimage 和一份旧的 fsiamge.
7. 总结: NN内存中的数据 = 磁盘上正在使用的 fsimage + 正在使用的 edits 文件.

### FsImage 和 Edits

**Edits** 记录各种操作信息.

**FsImage** 记录元数据.

**HDFS的目录结构怎么维护的？**

在fsimage中通过 `<inode>` 进行维护:

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

**对于文件的元数据信息来说, fsimage 中看不到块维护到哪些 dn 节点**

每个文件的块在哪些 dn 节点维护, 是 dn 主动上报给 nn 的, 因此在 nn 的内存中是有记录的. 但是在磁盘的fsimage 中是没有记录的.



# MR

## 核心思想

### MapReduce 的核心思想

先分 (map) 后合 (reduce)

### MapReduce核心思想初认识

1. 首先待计算的数据会先生成切片(逻辑上对数据进行划分) , 生成的切片个数对应着要启动多少个MapTask进行Map阶段的计算.
2. 多个MapTask是并行运行的，互不相干. 
3. 在每个MapTask中对数据的处理要考虑到很多细节, 是否有分区， 如何排序， 数据如何写磁盘等..
4. 多个MapTask计算完成后，每个MapTask都会有输出的数据
5. 会根据分区的个数决定启动多少个ReduceTask(逻辑上来说), 实际上是 启动多少个ReduceTask就会生成多少个分区.
6. 每个ReduceTask会到每个MapTask中拷贝自己所要处理的数据,说白了就是对应的分区的数据.
7. 每个ReduceTask最终也会输出最后的结果.



## hadoop 序列化

### 什么是序列化及反序列化?

序列化: 将内存中的对象以字节序列的方式写到文件中

反序列化: 将文件中的字节序列读取到内存, 并构造成对应的对象. 

### 为什么需要有序列化和反序列化?

对象中的数据需要**持久化**或**网路中传输**等 ...

### java中如何实现序列化和反序列化?

实现序列化接口: `Serializable` 

提供序列化标识号: `serialVersionUID`

对象流: `ObjectInputStream,  ObjectOutputStream`

### 为什么hadoop不用java的序列化?

对于hadoop 来说, Java 的序列化框架是**重量级**的. 一个对象在进行序列化时, 除了要关心对象的数据之外 (最重要的) , 还要附带很多额外的数据 (继承结构等).

因为 Hadoop 本身就是处理大量数据的, 当涉及到要序列化和反序列化时, 在能保证重要数据序列化的前提下, 尽可能减少无关紧要的数据. 因此 hadoop 提供了自己的序列化框架 `Writable`.

### Writable 接口 

`write()`: 序列化方法

`readFields()`: 反序列化方法

### 一个类实现hadoop序列化的步骤

1. 实现 `writable` 接口;
2. 实现 `write()` 和 `readFields()` 两个抽象方法. 注意: `write()` 写出的属性的顺序 (A->B->C) 与 `readFields()` 读回来的顺序要一致 (A->B->C);
3. 提供无参数构造器 (反序列的时候会反射调用无参数构造器来构造对象);
4. 建议重写 `toString()` 方法. 如果该类的对象会作为最终 MR 输出的结果来用的话, 会调用到该类的对象的`toString()` 进行打印;
5. 如果说该类会作为 MR 过程中的 key 来使用, 需要实现 Comparable 接口，并重写 `compreTo()` 方法.



## MapReduce 的过程

### 数据流

```
输入数据 -->  InputFormat --> Mapper --> shuffle --> Reducer --> OutputFormat --> 输出数据
```

### 过程

简单划分: Map 阶段 - shuffle阶段 - Reduce 阶段

### 过程

代码角度划分:

MapTask 类中的 `run()` 方法:  `MapTask : map(67%) sort(33%)`

```java
if (isMapTask()) {
	// If there are no reducers then there won't be any sort. Hence the map 
	// phase will govern the entire attempt's progress.
	
    if (conf.getNumReduceTasks() == 0) {
    	mapPhase = getProgress().addPhase("map", 1.0f);
    } else {
    	// If there are reducers then the entire attempt's progress will be 
    	// split between the map phase (67%) and the sort phase (33%).
    	mapPhase = getProgress().addPhase("map", 0.667f);
    	sortPhase  = getProgress().addPhase("sort", 0.333f);
    }
}
```

ReduceTask 类中的 `run()` 方法:  `ReduceTask :  copy   sort   reduce `

```java
if (isMapOrReduce()) {
    copyPhase = getProgress().addPhase("copy");
    sortPhase  = getProgress().addPhase("sort");
    reducePhase = getProgress().addPhase("reduce");
}
```

MapReduce的代码角度得到的过程结果为:

```
map --> sort --> copy --> sort --> reduce 
```



## InputFormat 数据输入

### 重要的方法

`getSplits()`: 生成切片的方法.
`createRecordReader()`: 创建 `RecordReader` 对象, 真正负责数据读取的对象.

### 重要的子抽象类  FileInputFormat

`getSplits()`: 做出了具体的实现.
`createRecordReader()`: 没有做任何的改动.
`isSplitable()`: 当前输入的数据集是否可切分.

### FileInputFormat 的具体实现类

#### TextInputFormat

MapReduce 默认使用的 InputFormat.

切片的规则:  用的就是父类 FileInputFormat 中的切片规则.

读取数据: LineRecordReader 按行读取数据.

#### CombineTextInputFormat

用于小文件过多的场景, 解决过多的小文件最终生成太多的切片的问题.

在 Driver 中设置:

```java
job.setInputFormatClass(CombineTextInputFormat.class);
CombineTextInputFormat.setMaxInputSplitSize(job,20971520); //20M
```

会按照设置的虚拟存储大小进行输入数据的逻辑上的规划:

- 如果文件的大小小于 MaxInputSplitSize, 则文件规划成一个;
- 如果文件的大小 大于MaxInputSplitSize, 但是小于 MaxInputSplitSize \* 2, 则文件规划成两个 (对半分);
- 如果文件的大小大于 MaxInputSplitSize \* 2, 先按照 MaxInputSplitSize 的大小规划一个, 剩余的再进行规划;
- 最终按照 MaxInputSplitSize 大小来生成切片;
- 将规划好的每个虚拟文件逐个累加, 只要不超过 MaxInputSplitSize 大小，则都是规划到**一个切片**中的.



## 切片 

### 相关两个概念

**块**: HDFS 存数据的单位. 是把要存储到 HDFS 的文件, 以设置好的块的大小从物理上将文件切成 N 个块.
**切片**: MapReduce 计算数据的单位. 是把要在 MR 中计算的数据从逻辑上按照切片的大小, 划分成 N 个切片.

### 切片的大小

切片的大小默认情况下等于块的大小. 

### 切片的源码解读

`FileInputFormat` 类中的 `getSplits()` 方法: 

**I:**

```java
long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));   // 1 
```
相关配置项: `"mapreduce.input.fileinputformat.split.minsize"="0"`

```java
long maxSize = getMaxSplitSize(job);  // Long.MAX_VALUE
```
相关配置项: `"mapreduce.input.fileinputformat.split.maxsize"`, 默认没有配置这一项.

**II:**

```java
long blockSize = file.getBlockSize();  //获取文件的块大小   
```
如果是集群环境, 获取到的就是集群中设置的块大小, 如果是本地环境, 本地默认的块大小 32 M (33554432).

**III:**

```java
                                  32M        1        Long.MAX_VALUE
long splitSize = computeSplitSize(blockSize, minSize, maxSize);

==> return Math.max(minSize, Math.min(maxSize, blockSize));
//想让切片大小调大, 增加 minSize
//想让切片大小调小, 减小 maxSize
```

**IV:**

```java
while (((double) bytesRemaining)/splitSize > SPLIT_SLOP)  // SPLIT_SLOP = 1.1 
```

如果剩余待切片的数据除以块大小, 大于 1.1, 才会继续切片, 如果不大于, 则直接将剩余的数据生成一个切片.

**切片的总结**

1. 每个切片都需要由一个 MapTask 来处理, 也就意味着在一个 MR 中, 有多少个切片, 就会有多少个 MapTask;
2. 切片的大小默认情况下等于块的大小;
3. 切片的时候每个文件单独切片, 不会整体切片;
4. 切片的个数不是越多越好, 也不是越少越少, 按照实际情况, 处理成合适的切片数.



## 分区

### 分区的概念

将数据按照条件输出到多个文件中.

### 为什么设置 reduce 的个数可以实现分区的效果

**I:**

在 MapTask 类中的 `NewOutputCollector()` 方法中: 

```java
partitions = jobContext.getNumReduceTasks();  // 获取reduce的个数， reduce的个数在driver中设置的
if (partitions > 1) {
    // 如果说 reducce 的个数大于1, 会尝试获取一个分区器类, 通过mapreduce.job.partitioner.class 参数获取.
    // 默认 mapreduce.job.partitioner.class 没有配置, 则直接返回 HashPartitioner.class.
    // 当然还有一种可能就是我们自己设置过分区器类, 则此处会获取到我们自己设置的分区器类。
    partitioner = (org.apache.hadoop.mapreduce.Partitioner<K,V>)
        ReflectionUtils.newInstance(jobContext.getPartitionerClass(), job);
} else {
    // 如果 reduce 的个数不大于 1, 最终的分区号就是固定的 0 号分区.
    partitioner = new org.apache.hadoop.mapreduce.Partitioner<K,V>() {
        @Override
        public int getPartition(K key, V value, int numPartitions) {
            return partitions - 1;
        }
    };
}
```

**II:**

获取到的分区器在哪里使用?

在 MapTask 的中的 NewOutputCollector 内部类中的 `write()` 方法:

```java
public void write(K key, V value) throws IOException, InterruptedException {
	collector.collect(key, value,
	partitioner.getPartition(key, value, partitions));
	// 将 k-v 收集到缓冲区的时候, 要计算出来 k-v 对应的分区号
}
```

### 分区的数据是如何分的?

- 数据的分区由分区器 (Partitioner) 来决定.	
- Hadoop 有默认的分区器对象 HashPartitioner. HashPartitioner 会按照 k 的 hash 值对 Reduce 的个数进行取余操作得到 k 所对应的分区.
- hadoop 也支持用户自定义分区器.

### 默认的分区器 HashPartitioner

默认的分区器就会按照 k 的 hashcode 值先对 Integer 的最大值做 & 运算, 再对 reduce 的个数取余, 得到分区号:

```java
public int getPartition(K key, V value,
int numReduceTasks) {
return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
}
```

### 自定义分区器

1. 自定义分区器继承 Partitioner 类, 并重写 `getPartition()` 方法.
2. 在 Driver 类中设置使用 `job.setPartitionerClass(PhoneNumPartitioner.class);`
3. 设置 reduce 的个数: 正常情况下, 根据分区器业务来决定设置多少个, 说白了就是分区器的逻辑会生成多少个分区, 则设置的多少个 reduce.

### 分区使用的注意事项

1. reduce 个数的设置: 如果不设置, reduce 的个数默认为 1, 则最终的分区号是固定的 0; 如果 1 < reduce < 分区数, 报错; 如果 reduce > 分区数, 不报错, 多出的 reduce 空跑一趟; 最佳 reduce 个数就设置为实际的分区数.
2. 分区号只能从 0 开始, 逐一累加.



## Shuffle 与排序

### shuffle 关键点

1. map 方法之后, reduce 方法之前的处理过程就是 shuffle 过程;
2. map 方法写出去的 k-v, 会被一个收集线程收集到缓冲区中;
3. 缓冲区的大小默认是 100 M, 达到 80% 发生溢写;
4. 缓冲区中记录的是 k-v,  k-v 的下标,  k-v 的分区等;
5. 溢写的时候, 是按照 k-v 的**分区**进行排序 (**快排，只排索引**), 再按照**分区溢写**.    --> map 端的第一次排序
6. 每个 MapTask 有可能发生多次溢写, 最终需要将多次溢写的文件归并成一个大的文件.    --> map 端的第二次排序
7. 在**溢写**和**归并**过程中, 都可以采用 **combiner**;
8. 每个 ReduceTask 按照所要处理的**分区**, 到每个 MapTask 中拷贝**对应的分区的数据**. 拷贝过程中, 先放内存, 放不下写磁盘. 等数据全部都拷贝过来后, 进行**归并**排序.     --> reduce 端的排序
9. reduce 端排好序的数据进行分组, 然后进入 reduce 方法进行业务处理.

### 排序

**I:**

排序是MR中最重要的操作之一:

在 MapTask 中有两次排序: **溢写前排序**, **归并排序**;

在 ReduceTask 中有 1 次排序: **归并排序**.

**II:**

在MR中, 排序是默认的行为. 默认会对 K-V 中的 K 进行排序. 默认采用**字典序**进行排序.

**III:**

排序的分类:

1. 全排序: 所有的数据整体排序, 要求只能有一个分区, 一个 reducer.
2. 区内排序: 每个分区内的数据整体排序.
3. 辅助排序: 分组排序.
4. 二次排序: 比较规则中用到两个条件.

**IV:**

排序的前提是能够进行比较, Java 中的比较接口和比较器:

| 类型            | Comparable         | 实现方法    |
| --------------- | ------------------ | ----------- |
| java 比较接口   | Comparable         | compareTo() |
| java 比较器     | Comparator         | compare()   |
| hadoop 比较接口 | WritableComparable | compareTo() |
| hadoop 比较器   | WritableComparator | compare()   |

**V:**

Hadoop 排序时如何比较的: 在 MapTask 中的 MapOutputBuffer 类中的 `init()` 方法中

```java
comparator = job.getOutputKeyComparator();  // 获取key的比较器对象

public RawComparator getOutputKeyComparator() {
	Class<? extends RawComparator> theClass = getClass(
    JobContext.KEY_COMPARATOR, null, RawComparator.class);
	// 参数: mapreduce.job.output.key.comparator.class  默认没有配置

    if (theClass != null)
		return ReflectionUtils.newInstance(theClass, this);  //如果能通过参数获取到, 则通过反射创建比较器对象
     
	// 如果通过参数获取不到, 则获取到在 driver 中设置的 map 的输出的 key 的类型
	// 并判断 key 的类型是否属于 WritableComparable 类型
	// 再尝试为 key 获取比较器对象.
	return WritableComparator.get(getMapOutputKeyClass().asSubclass(WritableComparable.class), this);
}
   
public static WritableComparator get(
    Class<? extends WritableComparable> c, Configuration conf) {
    // 从 comparators 中尝试获取 key 的比较器对象
    // 如果 key 是我们自己定义的类型, 则获取不到
    // 如果 key 是 hadoop 的序列化类型, 例如 Text, Intwriable等, 则能获取到.
    WritableComparator comparator = comparators.get(c);
    if (comparator == null) {
        // force the static initializers to run
        forceInit(c);  //强制进行类加载
        // look to see if it is defined now
        comparator = comparators.get(c);   // 再次进行获取
        // if not, use the generic one
        if (comparator == null) { 
            // 如果还获取不到，则直接 new 一个对象出来。
            comparator = new WritableComparator(c, conf, true);
        }
    }
    // Newly passed Configuration objects should be used.
    ReflectionUtils.setConf(comparator, conf);
    return comparator;
}
```
**总结**:

- Hadoop 自身的序列化类型, 在类加载时会把类型及对应的比较器对象注册到 WritableCompartor 中的 comparators 这个 Map 中;
- 如果 key 是我们自己定义的类型, 则我们必须要为该类型提交比较器对象;
- 不管是 Hadoop 自身的序列化类型还是我们自己定义的类型, 只要作为key来使用, 必须要有对应的比较器对象才能够进行比较, 才能实现排序.

**VI:**

比较器 WritableCompartor: 作为 key 来使用的类型, 需要提供比较器对象, 还要求 key 的类必须是 WritableComparable 类型.

1. `public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)`: 底层的实现, 我们不动, 会调用第2个方法;
2. `public int compare(WritableComparable a, WritableComparable b)`: 可以重写, 实现比较规则;
3. `public int compare(Object a, Object b)`: 还是会调用到第 2 个方法;

**VII:**

比较接口  WritableComparable: 

1. 要求所有作为 key 来使用的类型, 都需要实现 WritableComparable 接口;
2. 该接口中可以通过 `compareTo()` 定义比较规则.

**VIII:**

排序总结: 

1. 一个接口和一个类: 接口:  WritableComparable 定义默认的比较规则; 类: WritableCompartor 一般用于定义临时比较规则.

2. Hadoop 的排序, 都是对 key 的排序. 排序时需要比较, 比较的时候都是用 key 类型对应的比较器对象来进行比较.

3. 对于 key 的类型来说, 如果提供了对应的比较器对象, 则使用我们自己提供的; 如果没有提供对应的比较器对象, 则 Hadoop 会帮我们创建一个比较器对象.

4. Hadoop 对 key的比较, 默认调用的是**比较器类**中的 `compare(WritableComparable a, WritableComparable b)` 方法.

   第一种情况: 如果自定义的比较器中, 重写了 `compare()` 方法, 则使用重写后的方法进行比较.

   第二种情况: 如果自定义的比较器中, 没有重写 `compare()` 方法, 则使用的是 WritableCompartor 的`compare(WritableComparable a,WritableComparable b)` 方法.

   而在此方法中, 默认的实现是 `a. compareTo(b)` , 因此会调用到 key 的类中的 `compareTo()` 方法进行比较.

   第三种情况: 如果没有提供比较器对象, 则 Hadoop 默认帮我们创建一个比较器对象, 但是 Hadoop 默认创建的比较器对象还是使用 WritableCompartor 的 `compare(WritableComparable a,WritableComparable b)` 方法, 而该方法中默认的实现 `a. compareTo(b)` , 所以最终还是调用到 key 的类中的 `compareTo()` 方法进行比较.

5. 实际使用: 按照 Hadoop 的设计来说, 我们需要提供比较器对象 WritableComparator, 需要实现WritableComparable. 按照我们实际使用(偷懒), 不用提交比较器对象, 直接实现 WritableComparable 接口, 重写 `compareTo()` 即可. 一般来讲, WritableComparable 是用来定义**默认**的比较规则的, WritableComparator 用来定义**临时**的比较规则的.

**IX:**

全排序和区内排序:

全排序就是将排序的规则定义好, 只有一个分区一个 reducer, 数据整体有序;

区内排序就是在全排序的基础之上, 加上自定义分区即可.

**X:**

**分组排序**: 数据在进入到 reducer 方法之前, 一定要保证数据是有序的, 才可以进行所谓的分组. 最终的效果
就是要保证相同 key 的多个 k-v 对进入到一个 reducer方法.

Hadoop 是如何进行分组比较的?  在 ReduceTask 中的 `run()`方法中:

```java
RawComparator comparator = job.getOutputValueGroupingComparator();

public RawComparator getOutputValueGroupingComparator() {
    Class<? extends RawComparator> theClass = getClass(
        JobContext.GROUP_COMPARATOR_CLASS, null, RawComparator.class);
    // 配置项:  mapreduce.job.output.group.comparator.class

    if (theClass == null) {
        return getOutputKeyComparator();
    }

    return ReflectionUtils.newInstance(theClass, this);
}

public RawComparator getOutputKeyComparator() {
    Class<? extends RawComparator> theClass = getClass(
        JobContext.KEY_COMPARATOR, null, RawComparator.class);
    // 配置项:  mapreduce.job.output.key.comparator.class
        if (theClass != null)
            return ReflectionUtils.newInstance(theClass, this);
    return WritableComparator.get(getMapOutputKeyClass().asSubclass(WritableComparable.class), this);
}
```

在分组比较时, Hadoop 会获取当前 key 的类型对应的**分组比较器对象**(自己配置); 如果获取不到, 则尝试获取当前 key 的类型对应的**排序比较器对象**; 如果还获取不到, 则 Hadoop 会创建一个比较器对象, 最终调用到 key 的类中的 `compareTo()` 方法.

**XI:**

**分组, 具体怎么实现的?**

1. 分组是真正的把所有的数据按照 key 提前都分成一组一组的吗？ 答: 不是.
2. 真正的分组是:首先要求数据必须是有序的. 每次使用当前正在处理的数据与下一条数据进行比较, 得到两个结论: 是否还有下一条数据以及下一条数据与当前数据是否为一个组(分组比较器对象来判断).

**XII:**

**Reducer 类中的 `reduce()` 方法中的 key 和 values 的设计:**

key 和 value 都是一个变量, 指向了内存中的一个对象, 每次进行迭代的时候 key  和 value 的变量值不变. 变的是变量指向的堆中对象的内容.

所以需 deepcopy utils. 用 Spring 的 `BeanUtils.class`.



## Combiner

### combiner的工作位置

k-v 从缓冲区中溢写到磁盘时可以使用 combiner (只要设置, 无条件使用);

每个 MapTask 的所有数据都从缓冲区写到磁盘后, 在进行归并的时候可以使用 combiner (满足条件使用, 溢写次数 >=3);

### Combiner 合并

目的就是在每个 MapTask 中将输出的 k-v 提前进行合并;

能够降低 map 到 reduce 传输的数量及 reduce 最终处理的数据量. 

### Combiner 使用限制

在不改变业务逻辑的情况下才能使用 Combiner.



# Yarn

## Yarn 的资源调度器

### 如何配置使用调度器

```xml
<property>
<name>yarn.resourcemanager.scheduler.class</name>
<value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
</property>
```

### FIFO 先进先出

目前很少使用甚至不用.

特点就是每个 job 排队等待执行, 排到队首的先执行, 对首的执行不完, 后面只能等待.

存在的比较严重的问题: 大 job 会严重拖慢小 job 完成的时间.

例如队首的 job 是需要很多资源的大 job, 后续的 job, 就算需要很少的资源, 但是也需要慢慢等待.

### Capacity Scheduler 容量调度器

1. 可以配置多条队列, 每条队列可以设置资源占比;
2. 每条队列的队首的job可以同时执行（从侧面解决了大Job拖慢小job完成时间的问题);
3. 每条队列的空闲资源可临时借调给别的队列使用;
4. 每条队列可以设置最小和最大资源占比;
5. 每条队列可以控制访问用户.

### Fair Scheduler 公平调度器

每条队列中的job共享队列的资源，在时间尺度上公平的获取到队列中的资源.

最大最小公平算法: 

    不加权: (关注点是job的个数)
        有一条队列总资源12个, 有4个job, 对资源的需求分别是: job1->1, job2->2, job3->6, job4->5
        第一次算:  12 / 4 = 3 
    	job1: 分3 --> 多2个 
    	job2: 分3 --> 多1个
    	job3: 分3 --> 差3个
    	job4: 分3 --> 差2个
        
        第二次算: 3 / 2  = 1.5 
    	job1: 分1
    	job2: 分2
    	job3: 分3 --> 差3个 --> 分1.5 --> 最终: 4.5 
    	job4: 分3 --> 差2个 --> 分1.5 --> 最终: 4.5 
        
        第n次算: 一直算到没有空闲资源
    
    加权:  
        有一条队列总资源16, 有4个job
        对资源的需求分别是:
        job1->4 ,  job2->2 , job3->10 , job4->4 
    	每个job的权重为:
    	job1->5 ,  job2->8 , job3->1 ,   job4->2	
        
        第一次算: 	16 / (5+8+1+2) =  1
        job1:  分5 --> 多1
        job2:  分8 --> 多6
        job3:  分1 --> 少9
        job4:  分2 --> 少2
             
        第二次算: 7 / (1+2) = 7/3
        job1: 分4
        job2: 分2
        job3: 分1 --> 分7/3 --> 少
        job4: 分2 --> 分14/3(4.66) -->多2.66
        
        第三次算: 
        job1: 分4
        job2: 分2
        job3: 分1 --> 分7/3 --> 分2.66
        job4: 分4
