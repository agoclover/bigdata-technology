package com.atguigu.myscala.day03

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/30 6:54 下午
 */
object TestAnonymousFunction {
  def main(args: Array[String]): Unit = {
    val ints = map(_ + 1, Array(1, 2, 3)) // 返回一个数组
    println(Array(1, 2, 3))
    println(ints.mkString)

    println("-----")
    println(map2(1, 10, _ - _))
  }

  def map(f: Int => Int, array: Array[Int]) = {
    for (item <- array) yield f(item) // 产生一个值, 这些值整体组成了 Array数组
  }

  def map2(x: Int, y: Int, f: (Int, Int) => Int): Int = {
    f(x, y)
  }
}
