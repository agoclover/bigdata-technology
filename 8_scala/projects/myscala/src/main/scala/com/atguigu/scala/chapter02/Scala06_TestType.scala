package com.atguigu.scala.chapter02

/**
  * Author: Felix
  * Date: 2020/6/29
  * Desc: 数据类型
  */
object Scala06_TestType {
  def main(args: Array[String]): Unit = {
    /*
    //======整数类型======
    //var b:Byte = 10
    //var b:Byte = 300
    //var b = 10L
    //var a:Double = 10.0
    //var a:Float = 10.0f
//    var c1: Char = 'a' + 1
//    println("c1=" + c1 )

    //（2）\t ：一个制表位，实现对齐的功能
    println("姓名\t年龄")

    //（3）\n ：换行符
    println("西门庆\n潘金莲")

    //（4）\\ ：表示\
    println("c:\\岛国\\avi")

    //（5）\" ：表示"
    println("同学们都说：\"大海哥最帅\"")
*/
  //Unit
  //  println(test())
    //Null
  //  var s:Student = null
  //  var a:Int = null
  //  （1）自动提升原则：有多种类型的数据混合运算时，系统首先自动将所有数据转换成精度大的那种数据类型，然后再进行计算。
  //      var a = 1 + 2.0
  //      println(a)
  //  （2）把精度大的数值类型赋值给精度小的数值类型时，就会报错，反之就会进行自动类型转换。
  //      var b:Int = 10
  //      //var c:Byte = b
  //      var d:Long = b
  //      println(d)
  //  （3）（byte，short）和char之间不会相互自动转换。
  //    var b:Byte = 10
  //    var c:Char = b
  //  （4）byte，short，char他们三者可以计算，在计算时首先转换为int类型。
  //    var a:Byte = 1
  //    var b:Short = 2
  //    var c:Char = 'A'
  //    var d = a + b + c

    //强制类型转换
    //java   (目标类型)变量
    //scala  变量.toXXX
    //var a:Int = 2.5.toInt
    //var a:Int = 2.5.toInt + 3.6.toInt
    //var a:Int = (2.5 + 3.6).toInt

    //值类型和String之间相互转换
    //（1）基本类型转String类型（语法：将基本类型的值+"" 即可）
    var a:Int = 10
    //val str: String = a + ""
    //val str: String = a.toString
    //println(a)
    //（2）String类型转基本数值类型（语法：s1.toInt、s1.toFloat、s1.toDouble、s1.toByte、s1.toLong、s1.toShort）
    var s:String = "10.3"
    val i: Int = s.toInt
    println(i)

  }
  /*def test(a:Int): Int ={
    if(a==1){
      throw new NullPointerException
    }else{
      return 10
    }
  }*/
}

//class Student{}
