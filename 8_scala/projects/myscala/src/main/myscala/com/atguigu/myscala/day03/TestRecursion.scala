package com.atguigu.myscala.day03

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/30 2:07 下午
 */
object TestRecursion {
  def main(args: Array[String]): Unit = {
    println(factorial1(5))
  }
  def factorial1(n:Int):Int={
    if(n == 1) 1
    else n * factorial2(n-1)
  }

  def factorial2(n:Int):Int={
    if(n == 1) return 1
    n * factorial2(n-1)
  }
}
