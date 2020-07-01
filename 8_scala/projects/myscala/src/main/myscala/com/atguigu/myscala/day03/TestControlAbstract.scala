package com.atguigu.myscala.day03

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/30 2:14 下午
 */
object TestControlAbstract {
  def main(args: Array[String]): Unit = {
    def f()={
      println("in f")
      1
    }

    foo(f) // 打印函数对象
  }

  def foo(a:()=>Int)={
    a // <- 这里是一个局部变量
    a() // <- 这里是函数执行
    println(a)  // <- 函数未执行
//    println(a())  // <- 函数执行
  }
//
//  def foo(a: =>Int)={
//    println(a)
//    println(a)
//  }
//
//  def foo(a:Int)={
//    println(a)
//    println(a)
//  }
}
