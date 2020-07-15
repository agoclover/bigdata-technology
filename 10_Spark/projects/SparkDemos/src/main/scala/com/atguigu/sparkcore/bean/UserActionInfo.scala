package com.atguigu.sparkcore.bean

/**
 * <p>用户行为信息类</p>
 *
 * <p>将一条用户行为信息封装成为一个样例类.
 * 具体属性如名所示.</p>
 *
 * @author Zhang Chao
 * @version spark_day5
 * @date 2020/7/11 7:02 下午
 */
case class UserActionInfo(
                         val date : String,
                         val user_id : Long,
                         val session_id : String,
                         val page_id : Long,
                         val action_time : String,
                         val search_keyword : String,
                         val click_category_id : Long,
                         val click_product_id : Long,
                         val order_category_ids : String,
                         val order_product_ids : String,
                         val pay_category_ids : String,
                         val pay_product_ids : String,
                         val city_id : Long
                         )
