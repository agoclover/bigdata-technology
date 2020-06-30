package com.atguigu.scala.chapter02

/**
  * Author: Felix
  * Date: 2020/6/28
  * Desc: 字符串输出
  */
object Scala04_TestStrOut {
  def main(args: Array[String]): Unit = {
    var name:String = "zhangsan"
    var bzr:String = "fangfang"
    //通过 + 对字符串进行拼接
    //println("在尚硅谷，有一个叫"+bzr+"的班主任老师在等"+name+"来学习")

    //通过printf进行格式化输出
    var salary = 30
    //printf("叫%s的老师对学生的要求是年薪到%d才行",bzr,salary)

    //对字符串进行原样输出
    var sql:String =
      s"""
        |select
        |	*
        |from
        |	user
        |where
        |	name = ${name}
      """.stripMargin
    println(sql)
  }
}
