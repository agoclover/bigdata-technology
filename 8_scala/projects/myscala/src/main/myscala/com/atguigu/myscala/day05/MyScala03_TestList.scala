package com.atguigu.myscala.day05

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/3 9:32 下午
 */
object MyScala03_TestList {
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3)
    val list2 = List(2, 3, 4)

//    val list3 = list1 :: list2 //若无指定泛型, 会将 list1 整体头插入 list2
    val list3 = list1 ::: list2
    list3.foreach(println)

  }
}
