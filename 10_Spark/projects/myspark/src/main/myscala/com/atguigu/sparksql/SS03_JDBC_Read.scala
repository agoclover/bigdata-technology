package com.atguigu.sparksql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/14 8:17 下午
 */
object SS03_JDBC_Read {
  def main(args: Array[String]): Unit = {
    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //方式1：通用的load方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/spark_test?useUnicode=true&characterEncoding=utf-8")
//      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load().show


//    //方式2:通用的load方法读取 参数另一种形式
//    spark.read.format("jdbc")
//      .options(Map("url"->"jdbc:mysql://hadoop202:3306/test?user=root&password=123456",
//        "dbtable"->"user","driver"->"com.mysql.jdbc.Driver")).load().show
//
//    //方式3:使用jdbc方法读取
//    val props: Properties = new Properties()
//    props.setProperty("user", "root")
//    props.setProperty("password", "123456")
//    val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop202:3306/test", "user", props)
//    df.show

    //释放资源
    spark.stop()
  }
}
