package com.atguigu.sparksql

import org.apache.spark.sql.expressions.{Aggregator}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn}

/**
 * <p>Spark SQL - UDAF</p>
 *
 * <p>自定义 UDAF 来实现求平均值</p>
 *
 * @author Zhang Chao
 * @version SparkSQL_day1
 * @date 2020/7/14 4:46 下午
 */
object SS01_UDAF2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkApp")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[User] = spark.read.json("/Users/amos/BigdataLearn/10_Spark/projects/myspark/input/test.json").as[User]
    ds.show


    val f: TypedColumn[User, Double] = MyAvgUdaf.toColumn.name("myAvg")

//    val f: TypedColumn[User, Double] = MyAvgUdaf.udaf

    ds.select(f).show()

    spark.stop()
  }
}

case class MyAvger(var sumAge:Long, var counts:Long){
  def finish : Double={sumAge.toDouble/counts}
}
case class User(name:String, age:Long)

object MyAvgUdaf extends Aggregator[User, MyAvger, Double]{
  override def zero: MyAvger = MyAvger(0L,0L)

  override def reduce(b: MyAvger, a: User): MyAvger = {
    b.sumAge += a.age
    b.counts += 1L
    b
  }

  override def merge(b1: MyAvger, b2: MyAvger): MyAvger = {
    b1.sumAge += b2.sumAge
    b1.counts += b2.counts
    b1
  }

  override def finish(reduction: MyAvger): Double = {
    reduction.finish
  }

  override def bufferEncoder: Encoder[MyAvger] = Encoders.product // case class

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}