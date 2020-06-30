package com.atguigu.scala.myscala

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/30 2:29 下午
 */
object TestMyWhile {
  def main(args: Array[String]): Unit = {
    var i: Int = 1
//    myWhile1(i <= 5) ({
//      println(i)
//      i += 1
//    })

    myWhile3(i<5,{println(i);i+=1})
  }

  // 通过函数柯里化实现
  def myWhile1(condition: => Boolean)(opt: => Unit): Unit = { // 这里不能用 Boolean 因为要传的是代码块不是值
    if (condition) {
      opt
      myWhile1(condition)(opt)
    }
  }

  // 不使用函数柯里化, 使用函数嵌套完成循环
  def myWhile2(condition: => Boolean): (=> Unit) => Unit= {
    def opt(o: => Unit)={
      if(condition) {
        o
        myWhile2(condition)(o)
      }
    }
    opt _
  }

  // 普通函数调用, 不使用函数柯里化和函数嵌套, 这样虽然看着简单但是看着不好看
  def myWhile3(condition : => Boolean, opt : => Unit): Unit ={
    if (condition) {
      opt
      myWhile3(condition, opt)
    }
  }
}
