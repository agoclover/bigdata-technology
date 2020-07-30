package com.atguigu.spark

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/18 2:41 下午
 */
case class UserAdAction(
                       timeStap :Long,
                       area: String,
                       city: String,
                       user_id: Int,
                       ad_id: Int
                       ){
  val day:Long = timeStap/ 1000 / (60 * 60 * 24)
  val hour:Long = timeStap/ 1000 / (60 * 60)
  val min:Long = timeStap/ 1000 / 60

}
