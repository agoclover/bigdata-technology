package com.atguigu.myspark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/8 3:25 下午
 */
object S08_Transformation_partitionBy {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val RDD: RDD[(Int, String)] = sc.makeRDD(List((1, "aaa"), (2, "bbb"), (3, "ccc")))

    RDD.mapPartitionsWithIndex{
      (index, datas)=>{
        println(index + " -> " + datas.mkString(", "))
        datas
      }
    }.collect()

    println("---")

    val resRDD = RDD.partitionBy(new HashPartitioner(3))
    resRDD.mapPartitionsWithIndex{
      (index, datas)=>{
        println(index + " -> " + datas.mkString(", "))
        datas
      }
    }.collect()

    // 关闭连接
    sc.stop()
  }
}

class MyPartition(partitions : Int) extends Partitioner {
  override def numPartitions: Int = partitions

  // 只对 key 来进行分区
  override def getPartition(key: Any): Int = key.hashCode() % numPartitions
}
