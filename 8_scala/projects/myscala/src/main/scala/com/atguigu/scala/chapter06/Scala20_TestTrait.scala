package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 特质自身类型
  */
object Scala20_TestTrait {

}

class User(val name: String, val age: Int)

trait Dao {
  def insert(user: User) = {
    println("insert into database :" + user.name)
  }
}

trait APP {
  _: Dao =>

  def register(user: User): Unit = {
    println("login :" + user.name)
    insert(user)
    //getStackTrace
  }
}

object MyApp extends APP with Dao{
  def main(args: Array[String]): Unit = {
    register(new User("bobo", 11))
  }
}

