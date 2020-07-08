package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc:  集合计算高级函数
  *   reduce 简化（归约）
  *   fold  折叠
  */
object Scala14_TestFun {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1,2,3,4)
   /*
   println(list.reduce((a: Int, b: Int) => {
      a + b
    }))
    */

    //reduce
    // 对集合中的元素进行聚合操作，reduce聚合的两个元素的数据类型一致，底层调用reduceLeft
    //println(list.reduce(_  +  _))
    //reduceLeft     聚合的两个元素数据类型可以不一致
    println(list.reduceLeft(_+_))
    //reduceLeft     聚合的两个元素数据类型可以不一致
    println(list.reduceRight(_+_))

    //val list: List[Int] = List(3,4,5,8,10)
    //println(list.reduce(_ - _)) //-24
    //println(list.reduceRight(_ - _))//6

    //折叠
    //第一个参数列表：聚合的外部元素
    //第二个参数列表：聚合的规则
    println(list.fold(0)(_ + _))
    println(list.foldLeft(0)(_ + _))
    println(list.foldRight(0)(_ + _))


  }
}
