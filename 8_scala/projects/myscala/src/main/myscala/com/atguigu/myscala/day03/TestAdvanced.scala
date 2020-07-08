package com.atguigu.myscala.day03

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/30 6:19 下午
 */
object TestAdvanced {
  def main(args: Array[String]): Unit = {
    var i = 1
    myWhile3(i < 10,  {
      println(i)
      i += 1
    })
  }

  def myWhile1(condition: => Boolean)(opt: => Unit): Unit = {
    if (condition) {
      opt
      myWhile1(condition)(opt)
    }
  }

  def myWhile2(condition : => Boolean) : (=>Unit)=>Unit = {
    def opt(op : => Unit)={
      if (condition){
        op
        myWhile2(condition)(op)
      }
    }
    opt _
  }

  def myWhile3(condition: => Boolean, opts : => Unit): Unit = {
    if (condition){
      opts
      myWhile3(condition, opts)
    }
  }
}
