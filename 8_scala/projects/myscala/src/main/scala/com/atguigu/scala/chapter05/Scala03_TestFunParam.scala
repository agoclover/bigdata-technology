package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/29
  * Desc: 函数的参数
  */
object Scala03_TestFunParam {
  def main(args: Array[String]): Unit = {
    //（1）可变参数
    def f1(s:String*): Unit ={
      println(s)
    }
    //f1("aaa","bbbb","ccc")
    //f1()
    //（2）如果参数列表中存在多个参数，那么可变参数必须放置在最后
    //def f2(name:String*,addr:String): Unit ={}
    //def f2(addr:String,name:String*): Unit ={}
    //在一个方法的参数列表中，至多只能有一个可变长参数
    //def f2(addr:String*,name:String*): Unit ={}

    //（3）参数默认值，一般将有默认值的参数放置在参数列表的后面
    //def f3(name:String,sex:String="男"): Unit ={
    //  println(name +":::" + sex)
    //}
    //如果参数有默认值，那么在调用方法的时候，可以不传递该参数
    //f3("xitianyu")
    //如果调用方法的时候，给默认值也传递的参数，那么默认值会被覆盖
    //f3("fangfang","女")

    //（4）带名参数
    //建议：如果参数列表中有参数默认值，那么建议将带有默认值得参数放置在最后
    def f4(sex:String="男",name:String,age:Int = 20,add:String): Unit ={
      println(name +":::" + sex)
    }

    f4(name="zhangchao",add = "shanghai")
  }
}
