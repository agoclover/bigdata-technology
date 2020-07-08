package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 元组  将多个无关数据封装为一个整体进行操作
  */
object Scala09_TestTuple {
  def main(args: Array[String]): Unit = {
    //创建元组对象    默认元组元素最大不超过22个
    val tuple: (Int,String,Int) = (1000,"yuanyuan",20)

    //方法元组中元素
    //println(tuple._1)
    //println(tuple._2)
    //println(tuple._3)
    //通过索引访问元组中的元素
    //println(tuple.productElement(1))
    //通过迭代器访问元组 中的二元素
    //for (elem <- tuple.productIterator) {
    //  println(elem)
    //}

    //val map: Map[String, Int] = Map("a"->1,"b"->2,"c"->3)
    //Map集合是存放kv类型的键值对，可以用元组进行表示；只不过元组中元素的个数是固定的2个，我们称之为对偶元组
    val map: Map[String, Int] = Map(("a",1),("b",2),("c",3))

    map.foreach(println)
  }
}
