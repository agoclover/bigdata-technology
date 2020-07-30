package com.atguigu.myspark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/10 10:18 上午
 */
object S02_TransaKeyValue_sortByKey {
  implicit object MyOrdering extends Ordering[Student]{
    override def compare(x: Student, y: Student): Int = {
      val res = x.name.compareTo(y.name)
      if (res == 0){
        return x.age.compareTo(y.age)
      }
      res
    }
  }
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

//    sc.makeRDD(List((5,"aa"), (3,"ff"),(1,"gg"),(9,"dd"),(2,"gg")),2)
//        .sortByKey(true)
//        .collect()
//        .foreach(println)
//
//    sc.makeRDD(List((2,"aa"), (3,"ff"),(1,"gg"),(3,"dd"),(2,"gg")),2)
//      .sortByKey(true)
//      .collect()
//      .foreach(println)

    val s1 = new Student("aa", 12)
    val s2 = new Student("cc", 10)
    val s3 = new Student("bb", 9)
    val s4 = new Student("aa", 20)

    /*
    可以通过隐式转换使 Student 类实现 Ordering 接口, 相当于实现 Comparator
    或者直接使 Student 类实现 Ordered 接口, 相当于实现 Comparable
     */
    val RDD: RDD[Student] = sc.makeRDD(List(s1, s2, s3, s4))
    val RDD2: RDD[Student] = RDD.sortBy(stu => stu.name)
    RDD
        .collect()
        .foreach(println)

    // 关闭连接
    sc.stop()
  }
}

class Student(var name:String, var age:Int) extends Ordered[Student]{
  override def compare(that: Student): Int = {
    val res = this.name.compareTo(that.name)
    if(res == 0){
      return this.age.compareTo(that.age)
    }
    res
  }
}
