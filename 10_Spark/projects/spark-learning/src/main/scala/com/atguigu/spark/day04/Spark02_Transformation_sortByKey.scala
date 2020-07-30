package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/10
  * Desc: 转化算子_sortByKey    按照key进行排序
  */
object Spark02_Transformation_sortByKey {
  /*
  implicit object MyOrdering extends Ordering[Student]{
    override def compare(x: Student, y: Student): Int = {
      var res:Int = x.name.compareTo(y.name)
      if(res==0){
        res = y.age - x.age
      }
      res
    }
  }
  */
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    //按照key升序排序
    //val newRDD: RDD[(Int, String)] = rdd.sortByKey()
    //按照key降序排序
    //val newRDD: RDD[(Int, String)] = rdd.sortByKey(false)
    //按照value进行排序
    //val newRDD: RDD[(Int, String)] = rdd.sortBy(_._2)

    val std1: Student = new Student("aa",10)
    val std2: Student = new Student("dd",30)
    val std3: Student = new Student("bb",40)
    val std4: Student = new Student("cc",20)
    val std5: Student = new Student("aa",20)

    val stdRDD: RDD[Student] = sc.makeRDD(List(std1,std2,std3,std4,std5))
    val newRDD: RDD[Student] = stdRDD.sortBy(std=>std)
    newRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}

class Student(var name:String,var age:Int) extends Serializable with Ordered[Student]{

  override def toString = s"Student($name, $age)"

  override def compare(that: Student): Int = {
    var res:Int = this.name.compareTo(that.name)
    if(res==0){
      res = that.age - this.age
    }
    res
  }
}
