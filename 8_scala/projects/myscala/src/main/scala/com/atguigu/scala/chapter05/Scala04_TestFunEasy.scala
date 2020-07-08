package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc:  函数至简原则：能省则省
  */
object Scala04_TestFunEasy {
  def main(args: Array[String]): Unit = {
    //（0）标准的函数的定义以及调用方式
    //def f0(name:String): String ={
    //  return "hello:" + name
    //}
    //（1）return可以省略，Scala会使用函数体的最后一行代码作为返回值
    //def f1(name:String): String ={
    //  "hello:" + name
    //}
    //（2）如果函数体只有一行代码，可以省略花括号
    //def f2(name:String): String = "hello:" + name


    //（3）返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    //def f3(name:String)= "hello:" + name

    //（4）如果有return，则不能省略返回值类型，必须指定
    //def f4(name:String) ={
    //  return "hello:" + name
    //}
    //（5）如果函数明确声明unit，那么即使函数体中使用return关键字也不起作用
    //def f5(name:String): Unit ={
    //  return "hello:" + name
    //}

    //（6）Scala如果期望是无返回值类型，可以省略等号   没有返回值的函数称之为过程
    //def f6(name:String){
    //  println(name)
    //}

    //（7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    //def f7(): Unit ={
    //  println("xxxx")
    //}
    //f7
    //f7()
    //（8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    //def f8: Unit ={
    //  println("XXXX")
    //}
    //f8
    //f8()


    //（9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
    //函数： 参数列表 返回值     Int、String、函数
    //函数的声明语法    参数列表=>返回值

    def f10(s:String): Unit ={
      println(s)
    }

    def f9(f:String=>Unit): Unit ={
      f("atguigu11")
    }

    //f9(f10)

    //lambda 表达式    Java：参数 ->{函数体}  Scala:参数 =>{函数体}
    //f9((s:String)=>{println(s)})
    //传递匿名函数至简原则：
    //（1）参数的类型可以省略，会根据形参进行自动的推导
    //f9((s)=>{println(s)})
    //（2）类型省略之后，发现只有一个参数，则圆括号可以省略；其他情况：没有参数和参数超过1的永远不能省略圆括号。
    //f9(s=>{println(s)})
    //（3）匿名函数如果只有一行，则大括号也可以省略
    //f9(s=>println(s))
    //（4）如果参数只出现一次，则参数省略且后面参数可以用_代替
    //f9(println(_))
    //（5） 如果可以推断出来是一个函数，那么(_)也可以省略
    //f9(println)

    //至简原则练习   通过匿名函数，计算10和20的和，并进行简化
    def f11(f:(Int,Int)=>Int): Int ={
      f(10,20)
    }

    //def f12(a:Int,b:Int): Int ={
    //  a + b
    //}

    //println(f11((a:Int,b:Int)=>{a+b}))
    //println(f11((a,b)=>a+b))
    println(f11(_+_))
  }
}
