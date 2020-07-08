package com.atguigu.myscala.day06

import scala.collection.mutable.ListBuffer

/**
 * <p>WordCount 案例, 复杂情况</p>
 *
 * @author Zhang Chao
 * @version scala_day6
 * @date 2020/7/4 10:39 上午
 */
object MyWordCount_Hard {
  def main(args: Array[String]): Unit = {
    val list = List[(String, Int)](
      ("hello hello hello", 4),
      ("hello atguigu", 2),
      ("hello amos", 2),
      ("atguigu cathy", 3)
    )

    /**
     * 将一个包含若干单词的 list 统计出词频, 并按照要求顺序输出前面若干个.
     * 此方法最初是通过将 ("hello hello", 2) -> ("hello", 2), ("hello", 2) 简单一点的可以将 ("hello hello", 2) -> "hello hello hello hello"
     * @param list 需要被统计的 list 集合
     * @param sort 从大到小或从小到大, asc>0 则从小到大; asc<0 则从大到小.
     * @param num 输出的数量
     * @return
     */
    def wc1(list : List[(String, Int)], sort: Int, num: Int) =
      list
        .flatMap(x=>x._1.split(" ").map((_,x._2)))
        .groupBy(_._1)
        .map(x=>(x._1, x._2.reduce((x,y)=>(x._1,x._2+y._2))._2))
        .toList.sortBy(_._2 * sort)
        .splitAt(num)
        ._1

    def wc2(list : List[(String, Int)], sort: Int, num: Int) =
      list
        .flatMap{case (word, count) => word.split(" ").map((_, count))}
        .groupBy(_._1)
        .map{case (word, countList) => (word, countList.map(_._2).sum)}
        .toList.sortBy(_._2 * sort)
        .splitAt(num)
        ._1


    val resMap = wc1(list, -1, 3)
    resMap.foreach(println)
  }
}
