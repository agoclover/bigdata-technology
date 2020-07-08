package com.atguigu.scala.chapter08

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc: 匹配对象
  * //模式匹配，匹配对象原理
  *   >当使用模式匹配对对象类型数据进行匹配的时候，底层首先调用对象的unapply方法，获取对象的属性
  *   >获取到对象的属性之后，用属性和给定值进行equals比较
  */
/*object User{
  //根据属性创建对象
  def apply(name: String,age: Int): User = new User(name,age)

  //根据对象获取对象的属性
  def unapply(user: User): Option[(String, Int)] ={
    if(user == null){
      None
    }else{
      Some(user.name,user.age)
    }
  }
}*/
//样例类
case class User(var name:String,var age:Int)

object Scala03_TestMatchObject {
  def main(args: Array[String]): Unit = {
    var user:User = User("zhangsan",20)

    val res = user match {
      case User("zhangsan", 30) => "yes"
      case _ => "no"
    }
    println(res)
  }
}
