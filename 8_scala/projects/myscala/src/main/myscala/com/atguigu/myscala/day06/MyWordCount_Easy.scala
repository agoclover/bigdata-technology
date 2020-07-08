package com.atguigu.myscala.day06

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * <p>TWordCount 案例, 简单情况</p>
 *
 * @author Zhang Chao
 * @version scala_day6
 * @date 2020/7/4 8:50 上午
 */
object MyWordCount_Easy {
  def main(args: Array[String]): Unit = {
    val list = List[String]("hello hello hello", "hello atguigu", "hello", "atguigu", "atguigu amos", "hello", "atguigu", "cathy")

    /**
     * 将一个包含若干单词的 list 统计出词频, 并按照要求顺序输出前面若干个
     * @param list 需要被统计的 list 集合
     * @param asc 从大到小或从小到大, asc>0 则从小到大; asc<0 则从大到小.
     * @param num 输出的数量
     * @return
     */
    def wc1(list : List[String], asc: Int, num: Int) =
      list
        .flatMap(_.split(" "))
        .map((_, 1))
        .groupBy(_._1)
        .map(x=>(x._1, x._2.length))
        .toList
        .sortBy(_._2 * asc)
        .splitAt(num)
        ._1

    def wc2(list : List[String], sort: Int, num: Int) =
      list
        .flatMap(_.split(" "))
        .map((_, 1))
        .groupBy(_._1)
        .map{case (key, wordList) => (key, wordList.length)}
        .toList
        .sortBy{case (_, count) => count * sort}
        .splitAt(num)
        ._1

    val resMap = wc2(list, -1, 3)
    resMap.foreach(println)
  }
}
