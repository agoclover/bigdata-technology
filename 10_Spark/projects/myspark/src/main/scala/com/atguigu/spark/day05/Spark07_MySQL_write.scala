package com.atguigu.spark.day05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/11
  * Desc: 向MySQL数据库中写入数据
  */
object Spark07_MySQL_write {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("azhu",28),("menggu",22),("yuyan",20)))
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop202:3306/test"
    val userName = "root"
    val passWd = "123456"

    //将rdd中的数据保存到数据库表中
    /* 这种方式很不好，因为每循环一次都要创建一个连接对象，效率很低
    rdd.foreach{
      case (name,age)=>{
        //注册驱动
        Class.forName(driver)
        //建立连接
        val conn: Connection = DriverManager.getConnection(url,userName,passWd)
        //创建数据库操作对象
        val ps: PreparedStatement = conn.prepareStatement("insert into user(name,age) values(?,?)")
        //给占位符赋值
        ps.setString(1,name)
        ps.setInt(2,age)
        //执行SQL语句
        ps.executeUpdate()
        //释放资源
        ps.close()
        conn.close()
      }
    }
    */

    /*
    //将连接、ps对象的创建移到循环外面,但是在foreach算子中访问了外部变量ps，需要对ps进行序列化，但是
    ps不是我们提供的程序，不能直接对其进行操作。
    //注册驱动
    Class.forName(driver)
    //建立连接
    val conn: Connection = DriverManager.getConnection(url,userName,passWd)
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement("insert into user(name,age) values(?,?)")

    rdd.foreach{
      case (name,age)=>{
        //给占位符赋值
        ps.setString(1,name)
        ps.setInt(2,age)
        //执行SQL语句
        ps.executeUpdate()
      }
    }
    //释放资源
    ps.close()
    conn.close()*/



    rdd.foreachPartition(datas=>{
      //注册驱动
      Class.forName(driver)
      //建立连接
      val conn: Connection = DriverManager.getConnection(url,userName,passWd)
      //创建数据库操作对象
      val ps: PreparedStatement = conn.prepareStatement("insert into user(name,age) values(?,?)")

      datas.foreach{
        case (name,age)=>{
          //给占位符赋值
          ps.setString(1,name)
          ps.setInt(2,age)
          //执行SQL语句
          ps.executeUpdate()
        }
      }
      //释放资源
      ps.close()
      conn.close()
    })



    // 关闭连接
    sc.stop()
  }
}
