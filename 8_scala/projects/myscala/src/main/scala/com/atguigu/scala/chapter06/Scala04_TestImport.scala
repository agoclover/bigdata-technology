package com.atguigu.scala.chapter06


/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc:   Scala的Import导入
  * 1）和Java一样，可以在顶部使用import导入，在这个文件中的所有类都可以使用。
  * 2）局部导入：什么时候使用，什么时候导入。在其作用范围内都可以使用
  * 3）通配符导入：import java.util._
  * 4）给类起名：import java.util.{ArrayList=>JL} 可以避免不同包下有同名的类冲突
  * 5）屏蔽类：import java.util.{ArrayList =>_,_}  可以避免不同包下有同名的类冲突
  * 6）导入相同包的多个类：import java.util.{HashSet, ArrayList}
  * 7）导入包的绝对路径：new _root_.java.util.HashMap
  */
//import java.util.{Date,ArrayList}
//import java.util.{Date,ArrayList}
import java.util._
//import java.sql.{Date=>_,Array=>_,_}
import java.sql.{Date=>DD,Array=>_,_}
object Scala04_TestImport {
  def main(args: Array[String]): Unit = {
    new Date
    new DD(2020)
    new ArrayList
    new _root_.java.util.HashMap
  }
}

class HashMap{

}
