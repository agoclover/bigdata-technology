/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc: 嵌套风格的包管理方式
  *     -一个源文件中可以定义多个包
  *     -子包可以访问父包中的内容而不需要导包
  */
package aaa{
  package bbb{
    package ccc{
      object Scala03_TestPackage {
        def main(args: Array[String]): Unit = {
          println(shareValue)
          shareMethod()
        }
      }
    }
  }
  package object bbb{
    val shareValue="share1111"
    def shareMethod()={
      println("xxxxxx")
    }
  }
}


package ddd{
  package eee{
    object Test03{
      def main(args: Array[String]): Unit = {
        println("yyyy")
      }
    }
  }
}


package com {

  import com.atguigu.Inner //父包访问子包需要导包

  object Outer {
    val out: String = "out"

    def main(args: Array[String]): Unit = {
      println(Inner.in)
    }
  }

  package atguigu {

    object Inner {
      val in: String = "in"
      println()

      def main(args: Array[String]): Unit = {
        println(Outer.out) //子包访问父包无需导包
      }
    }
  }
}




