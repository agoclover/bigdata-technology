package com.atguigu.scala.chapter09

import java.io.FileNotFoundException

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc: 异常
  *   Java异常
  *     -体系结构
  *       Throwable
  *         *Error
  *         *Exception
  *           >编译时异常|受检异常
  *           >运行时异常|非受检异常
  *     -运行原理
  *       在程序执行的过程中，如果发生了异常，其实是通过throw关键字抛出对应的异常类型对象
  *       在程序中找到能够处理这段异常的代码块（catch捕获），如果不能对该异常进行处理，继续向上抛出
  *
  *     -异常处理的方式
  *       *try{
  *           可能发生异常的代码块
  *        }catch(FileNotFoundException e){
  * *           异常处理的代码块
  * *        }catch(Exception e){
  *           异常处理的代码块
  *        }finally{
  *           不管是否发生异常都会执行的代码
  *        }
  *
  *       *在声明方法的时候 通过throws关键字声明异常
  *   Scala异常
  *      -在Scala语言中，异常不区分编译时异常和运行时异常了
  *      -语法
  *         try{
  *         }catch{
  *           case e1:FileNotFoundException=>{}
  *           case e1:Exception=>{}
  *         }
  */
object Scala01_TestException {
  def main(args: Array[String]): Unit = {
    try{
      10/0
      println("没有发生异常")
    }catch {
      case e:ArithmeticException=>println("发生了算术异常")
        //scala语法支持将大范围异常类型放到前面，但是不建议这么做
      case e:Exception=>println("发生了异常")
    }finally {
      println("不管是否发生异常都执行")
    }

  }

  @throws(classOf[NumberFormatException])
  def f11()={
    "abc".toInt
  }

}
