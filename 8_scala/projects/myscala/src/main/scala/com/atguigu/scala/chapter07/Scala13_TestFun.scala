package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 集合计算高级函数
  */
object Scala13_TestFun {
  def main(args: Array[String]): Unit = {
    //（1）过滤    遍历一个集合并从中获取满足指定条件的元素组成一个新的集合
    //val list: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //需求1：获取集合中的偶数部分
   /* println(list.filter((elem: Int) => {
      elem % 2 == 0
    }))*/
    //list.filter(_%2 ==0)

    //（2）转化/映射（map） 将集合中的每一个元素映射到某一个函数

    //需求2：对集合中的元素*2
    //println(list.map(_ * 2))

    //（3）扁平化   将一个整体拆分为个体的过程    :::
    //val nestedList: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
    //println(nestedList.flatten)

    //需求：对当前集合中的元素进行处理，目标效果List(hello,world,hello,atguigu,hello,scala)
    //val wordList: List[String] = List("hello world", "hello atguigu", "hello scala")

    /*
    //第一步：对集合中的元素进行映射
    //val newList: List[Array[String]] = wordList.map((str: String) => {
    //  val arr: Array[String] = str.split(" ")
    //  arr
    //})


    val newList: List[Array[String]] = wordList.map(_.split(" "))
    //第二步：将数组中的元素进行拆分，拆为个体
    println(newList.flatten)
    */

    //（4）扁平化+映射 注：flatMap相当于先进行map操作，在进行flatten操作 集合中的每个元素的子元素映射到某个函数并返回新集合
    //println(wordList.flatMap(_.split(" ")))


    //（5）分组(group) 按照指定的规则对集合的元素进行分组
    //val list: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //需求：奇数一组，偶数一组
    //val resMap: Map[Int, List[Int]] = list.groupBy((elem:Int)=>elem%2)
    //println(resMap)

    val nameList = List("zhangpeng","zhangchao","zhangbao","wangxingy","wanggjian")
    //名字首字符相同的放到一组
    println(nameList.groupBy(_.charAt(0)))
  }
}
