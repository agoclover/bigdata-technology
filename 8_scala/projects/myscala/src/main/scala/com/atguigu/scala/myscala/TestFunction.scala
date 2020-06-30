package com.atguigu.scala.myscala

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/30 10:02 上午
 */
object TestFunction {
  def main(args: Array[String]): Unit = {
    // 至简原则练习
    def add(f : (Int, Int) => Int): Int = f(10, 20)
//    println(add((x, y) => x+y))
    val res = add(_ + _)
    println(res)

    // 高级函数特点3: 函数作为返回值传递
    var v3 = f1
    v3() // 以返回值赋值之后再调用
    f1()() // 直接调用

//    var aa = h((a,b)=> return) //? 返回不要用 return
    println(h((a,b)=> return).getClass)
    println("test")
  }

  def f1()={
    def f2():Unit={
      println("This is f2")
    }
    f2 _
  }

  def h(f : (Int, Int) => Int)={
    f(10, 20)
  }

}
