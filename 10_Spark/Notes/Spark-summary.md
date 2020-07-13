## RDD 中体现的设计模式 : Decorator

### I/O 流的读取

装饰器(Decorator)模式, 是一种在**运行期动态给某个对象的实例增加功能的方法**.

比如在 Java 标准库中, InputStream 是抽象类, FileInputStream 是它的一个实现类, 现在, 如果要给这个实现类增加缓冲功能和按照行来读取的功能, 不通过子类新创建一个个类, 而是把这些的附加功能，用 Decorator 的方式给一层一层地累加到原始数据源上, 最终通过组合获得我们想要的功能.

一般增加功能是通过继承原有的类, 来扩大原有类的功能, 但如果原有类增多, 或者增加新功能, 子类会爆炸式增长, 这种设计方式显然是不可取的.

比如:

```java
InputSteam fis = new FileInputStream("path");
InputStreamReader isr = new InputStreamReader(fis);
BufferedReader br = new BufferedReader(isr);
```

这种写法就体现了**装饰者设计模式**, 对**对象结构不作任何改动**, **同时给对象扩展功能**. 通过不断地转换流格式, 将流的功能进行扩展, 这就是装饰者设计模式.

### RDD的设计体现装饰者模式

RDD 之间具有依赖关系, 通过 RDD 转换实现, 一个 RDD 通过 转换算子 Transformations 可以生成一个新的RDD, 这样从第一个 RDD 转换到最后一个 RDD 之间的所有 RDD 具有血缘关系.

Driver 中定义的 RDD 的各种转换算子并不会立即执行, 它们只代表将一种 RDD 类型中的数据转换到了其他 RDD 类型中, 只有触发执行算子后才会真正执行所有 RDDs 进行计算.

比如, 在最简单的 WordCount 案例中, SparkContext 读取文件生成 HadoopRDD, 调用 flatMap 算子包装成 MapPartitionsRDD, 再调用 map 方法再包装一层 MapPartitionsRDD, 最后调用 reduceByKey 包装成 ShuffledRDD, 然后延迟到 collect 方法被触发, 才会真正去执行.

RDDs 之间存在依赖, RDD 的执行是按照血缘关系**延时计算**的. 如果血缘关系较长, 可以通过持久化 cache 或 persist RDD 来切断血缘关系, 这样需要数据或数据丢失时, 就**不再追溯血缘关系而是通过持久化层**去读取. 



# RDD

### 什么是 RDD, 对弹性, 分布式和数据集的认识. 

见 1.2

### RDD 的 5 个特性

见 1.3

### RDD 创建

1. **从集合中创建 RDD**. Spark主要提供了两种函数: parallelize 和 makeRDD, 这两个方法其实都是 new 了一个 ParallelCollectionRDD.
2. **从外部存储系统创建 RDD**. 比如 textFile 方法返回一个 HadoopRDD.
3. **一个 RDD 通过 Transformations 转换得到一个新的 RDD**.

### 分区规则

**以 scala 集合创建 RDD**

如果未指定分区数, 会有默认分区数, 如果在配置文件中配置了 `spark.default.parallelism` 的值, 则取出; 如果没有指定,  则取出 CPU 的总核数 `totalCores`. 

前面得到的数和 2 取最大值即为默认分区数.

**以外部文件创建 RDD**

通过本地文件一般是以 32M 切分, 得到的数和默认最小分区数 2 取最大值;

来自 HDFS 的文件就是以 HDFS 的块的大小来切分, 规则和 HDFS 切分数据一样, 同样得到的数和默认最小分区数 2 取最大值.



### RDD Operator

Transformations

	- V
	- 双 V
	- K - V

Action



# RDD Serializable



# RDD Dependecies

## 宽依赖和窄依赖

通过 `toDebugString` 查看 RDD 的血缘关系;

通过 `Dependencies` 查看 RDD 的依赖关系.

RDD 之间的依赖关系包括两种: 窄依赖和宽依赖, NarrowDependency 和 ShuffleDependency, 主要区别就是宽依赖即进行了 shuffle 操作的算子.

宽依赖对 Spark 去评估一个 transformations 有更加重要的影响, 比如对性能的影响.

## Job 调度

触发行动算子之后, 会由 Driver 端的 SparkContext.runJob(RDD, func, partitions, ...) 准备好若干参数, 然后由 DAGScheduler.runJob(RDD, func, partitions, ...) 再通过 DAGScheduler.submitJob(RDD, func, partitions, ...) 提交这一个 Job, 





# References

1. [装饰者模式——IO流和RDD的设计](https://blog.csdn.net/wx1528159409/article/details/86627794)
2. [廖雪峰 Java - 装饰器](https://www.liaoxuefeng.com/wiki/1252599548343744/1281319302594594)