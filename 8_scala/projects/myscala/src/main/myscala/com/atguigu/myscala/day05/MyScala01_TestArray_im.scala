package com.atguigu.myscala.day05

/**
 * <p>Seq - Array</p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/3 8:38 下午
 */
object MyScala01_TestArray_im {
  def main(args: Array[String]): Unit = {
//    val arr = new Array[Int](5)
    val arr = Array(1, 2, 3)

//    val it : Iterator[Int] = arr.iterator
//    while(it.hasNext){
//      println(it.next())
//    }
//
//    for (elem <- arr.iterator) {
//      println(elem)
//    }

    arr.foreach(println)

    println(arr.hashCode())
    val newArr = 10 +: arr
    arr :+ 1
    arr(0) = 11 //
    arr.foreach(println)
//
//    println(arr.hashCode())
//    arr.foreach(println)
//    newArr.foreach(println)
//
//    val newArr2 = arr ++ arr
//    println(newArr2.mkString(" "))


  }
}
