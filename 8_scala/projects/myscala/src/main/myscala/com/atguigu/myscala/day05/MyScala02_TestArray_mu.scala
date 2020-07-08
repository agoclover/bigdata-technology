package com.atguigu.myscala.day05

import scala.collection.mutable.ArrayBuffer

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/3 9:06 下午
 */
object MyScala02_TestArray_mu {
  def main(args: Array[String]): Unit = {
//    val arrMu = ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
//    arrMu.update(3, 66)
//    arrMu.foreach(println)

//    val ints = arrMu.-(1) // - 还是返回新的, -= 则是改变原来的
//    arrMu.foreach(println)
//    ints.foreach(println)
//
//    arrMu -= 1
//    arrMu.foreach(println)
//
//    arrMu.remove(3, 2) // 从 index = 3 开始, 删除两个
//    arrMu.foreach(println)
//
//    val arrIm = arrMu.toArray
//    val arr = arrIm.toBuffer
    val arrDim = Array.ofDim[Int](2, 3)
    arrDim(1)(1) = 199
    for(i <- 0 until arrDim.length; j <- 0 until arrDim(i).length){
      print(arrDim(i)(j) + "\t") // 能力有限
    }
  }
}
