package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}

/**
 * <p>Spark SQL - UDAF</p>
 *
 * <p>自定义 UDAF 来实现求平均值</p>
 *
 * @author Zhang Chao
 * @version SparkSQL_day1
 * @date 2020/7/14 4:46 下午
 */
object SS01_UDAF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkApp")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("/Users/amos/BigdataLearn/10_Spark/projects/myspark/input/test.json")

    df.createOrReplaceTempView("user")


//    val f = new MyAvgUDAF

//    spark.udf.register("myAgeAvg", f)

    spark.sql("select myAgeAvg(age) from user").show

    spark.stop()
  }
}

class MyAvgUDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(Array(StructField("age", IntegerType)))

  override def bufferSchema: StructType = StructType(Array(StructField("sum", LongType), StructField("count", LongType)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + input.getInt(0).toLong
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble/buffer.getLong(1)
}