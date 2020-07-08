package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc:通过读取文件创建RDD分区规则
  *   -分区数确定：文件的总字节数和指定的最小分区数是不是能够整除，如果能够整除，那么实际分区就是最小分区
  *     如果不能够整除，需要研究源代码。
0 	1	  2	  3	  4	  5	  6	  7	  8
a 	b 	c 	d 	e 	f 	g	  X 	X

9	  10	11	12	13	14
h 	i 	j 	k	  X	  X

15	16	17	18	19
l	  m	  n	  X	  X

20	21
o 	p

一共有22个字节，最小分区数是3,实际分区数4
0分区	:	abcdefg
1分区	:	hijk
2分区	:	lmn
			op
3分区	:	空

0 = {FileSplit@5955} "file:/D:/dev/workspace/bigdata-0317/spark-0317/input/2.txt:0+7"
1 = {FileSplit@5986} "file:/D:/dev/workspace/bigdata-0317/spark-0317/input/2.txt:7+7"
2 = {FileSplit@6036} "file:/D:/dev/workspace/bigdata-0317/spark-0317/input/2.txt:14+7"
3 = {FileSplit@6098} "file:/D:/dev/workspace/bigdata-0317/spark-0317/input/2.txt:21+1"

LineRecoreReader
start = split.getStart();
end = start + split.getLength();

  */
object Spark05_Partitions_file {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //输入数据1-4，每行一个数字,最小分区数设置为3    最终分区数是4个，  0->1,2  1->3,  2->4, 3->空
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input",3)

    //输入数据1-4，放在一行,最小分区数设置为3    最终分区数是4个，  0->1,2,3,4  1->空,  2->空, 3->空
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input",3)

    //输入数据1-4，1一行，2，3，4放在一行,最小分区数设置为3    最终分区数是3个，  0->1,  1->2,3,4,  2->空, 3->空
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input",3)

   /*
    abcdefg
    hijk
    lmn
    op
    */
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input\\3.txt",3)
    val rdd: RDD[String] = sc.textFile("/Users/amos/BigdataLearn/10_Spark/Projects/myspark/input",2)

    rdd.saveAsTextFile("/Users/amos/BigdataLearn/10_Spark/Projects/myspark/output")

    // 关闭连接
    sc.stop()
  }
}
