package com.atguigu.myscala.day03

/**
 * <p>作业</p>
 *
 * <p>
 * 1. 打印等腰三角形
 * 2. 打印9 9 乘法表 </p>
 *
 * @author Zhang Chao
 * @version scala_day2
 * @date 2020/6/30 8:35 上午
 */
object Homework {
  def main(args: Array[String]): Unit = {
    tri(9)
    drawNineNine(9)
  }

  /**
   * 等腰三角形
   * @param n
   */
  def tri(n :Int) = for(i <- 1 to n; space = n - i; star = 2 * i - 1)
//      println(s"%${n - i + 1}s" format "" + s"%${2 * i - 1}s" format " " replace(" ", "*"))
    println(" " * space + "*" * star) // 函数式编程的优势, python也可以这样


  /**
   * 99乘法表
   * @param row
   */
  def drawNineNine(row :Int):Unit = if (row > -1) {
      for (col <- 1 to row if col < 9)
        printf("%d*%d=%s\t",row, col, (row * col).toString)
      println; drawNineNine(row-1)
    }
}
