package com.atguigu.sparksql

import java.text.DecimalFormat
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
/*
+----+------------+------------+---------------------------------------------+
|area|product_name|click_counts|compositions                                 |
+----+------------+------------+---------------------------------------------+
|华东|商品_86     |371         |上海 : 16.44%, 杭州 : 15.90%, 其他 : 67.65%  |
|华东|商品_47     |366         |杭州 : 15.85%, 青岛 : 15.57%, 其他 : 68.58%  |
|华东|商品_75     |366         |上海 : 17.49%, 无锡 : 15.57%, 其他 : 66.94%  |
|西北|商品_15     |116         |西安 : 54.31%, 银川 : 45.69%                 |
|西北|商品_2      |114         |银川 : 53.51%, 西安 : 46.49%                 |
|西北|商品_22     |113         |西安 : 54.87%, 银川 : 45.13%                 |
|华南|商品_23     |224         |厦门 : 29.02%, 福州 : 24.55%, 其他 : 46.43%  |
|华南|商品_65     |222         |深圳 : 27.93%, 厦门 : 26.58%, 其他 : 45.50%  |
|华南|商品_50     |212         |福州 : 27.36%, 深圳 : 25.94%, 其他 : 46.70%  |
|华北|商品_42     |264         |郑州 : 25.00%, 保定 : 25.00%, 其他 : 50.00%  |
|华北|商品_99     |264         |北京 : 24.24%, 郑州 : 23.48%, 其他 : 52.27%  |
|华北|商品_19     |260         |郑州 : 23.46%, 保定 : 20.38%, 其他 : 56.15%  |
|东北|商品_41     |169         |哈尔滨 : 35.50%, 大连 : 34.91%, 其他 : 29.59%|
|东北|商品_91     |165         |哈尔滨 : 35.76%, 大连 : 32.73%, 其他 : 31.52%|
|东北|商品_58     |159         |沈阳 : 37.74%, 大连 : 32.08%, 其他 : 30.19%  |
|华中|商品_62     |117         |武汉 : 51.28%, 长沙 : 48.72%                 |
|华中|商品_4      |113         |长沙 : 53.10%, 武汉 : 46.90%                 |
|华中|商品_57     |111         |武汉 : 54.95%, 长沙 : 45.05%                 |
|西南|商品_1      |176         |贵阳 : 35.80%, 成都 : 35.80%, 其他 : 28.41%  |
|西南|商品_44     |169         |贵阳 : 37.28%, 成都 : 34.32%, 其他 : 28.40%  |
+----+------------+------------+---------------------------------------------+
only showing top 20 rows


Process finished with exit code 0

 */
/**
 * <p>Spark SQL Demo Case</p>
 *
 * <p>求不同区域热门商品前三的点击量和各个省市占比.</p>
 *
 * @author Zhang Chao
 * @version Spark_day8
 * @date 2020/7/15 6:20 下午
 */
object SSql_Solution {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSqlDemoProject")
      .enableHiveSupport()
      .getOrCreate()

    val agg = new MyAgg

    spark.udf.register("agg", functions.udaf(agg))

    // 1. 查询用户行为表, 过滤出点击行为, 和城市以及商品表进行连接查询
    // city_info : city_id, city_name, area
    // t1 -> area, product_name, city_name, *city_id
    spark.sql(
      """
        |select
        |    c.*,
        |    p.product_name
        |from
        |    db_hive.user_visit_action a
        |left join
        |    db_hive.city_info c
        |on
        |    a.city_id = c.city_id
        |left join
        |    db_hive.product_info p
        |on
        |    a.click_product_id = p.product_id
        |where
        |    a.click_product_id != -1;
        |""".stripMargin).createOrReplaceTempView("t1")

    // 2. 按照地区和商品名称分组，统计出每个商品在每个地区的总点击次数
    spark.sql(
      """
        |select
        |    t1.area,
        |    t1.product_name,
        |    count(*) as product_click_count,
        |    agg(t1.city_name) as compositions
        |from
        |    t1
        |group by
        |    t1.area, t1.product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    // 3. 利用开窗函数, 对每个区域内按照商品点击量排序
    spark.sql(
      """
        |select
        |    t2.*,
        |    ROW_NUMBER() over(partition by t2.area order by t2.product_click_count desc) rankNo
        |from
        |    t2
        |""".stripMargin).createOrReplaceTempView("t3")

    // 4. 利用 where 条件过滤取排名前三的产品, 得到 区域, 产品, 总点击, 排名这几个字段的按照区域分组, 按照点击量排序
    spark.sql(
      """
        |select
        |    t3.area,
        |    t3.product_name,
        |    t3.product_click_count as click_counts,
        |    t3.compositions
        |from
        |    t3
        |where
        |    t3.rankNo <= 3
        |""".stripMargin).show(false)

    spark.stop()


  }
}

// 在第二步会以 area 和 product_name 分组, 以此来计数(参数2) ↓
case class CityRemarkBuffer(var cityMap: mutable.Map[String, Long], var totalCount: Long) {
  def finish(): String = {
    val resMap: mutable.Map[String, Double] = mutable.Map()
    var remarkList: List[RemarkUnit] = cityMap.toList
      .sortBy(-_._2) //  降序排列
      .take(2)
      .map { case (cityName, count) => RemarkUnit(cityName, count.toDouble / totalCount) }
    if (cityMap.size > 2) {
      remarkList = remarkList :+ RemarkUnit("其他", remarkList.foldLeft(1D)(_ - _.rate))
    }

    remarkList.mkString(", ")
  }
}

case class RemarkUnit(cityName: String, rate: Double) {
  val formatter = new DecimalFormat("0.00%")

  override def toString: String = s"$cityName : ${formatter.format(rate)}"
}


class MyAgg extends Aggregator[String, CityRemarkBuffer, String] {
  override def zero: CityRemarkBuffer = CityRemarkBuffer(mutable.Map[String, Long](), 0L)

  override def reduce(b: CityRemarkBuffer, a: String): CityRemarkBuffer = {
    b.cityMap(a) = b.cityMap.getOrElse(a, 0L) + 1L
    b.totalCount += 1L
    b
  }

  override def merge(b1: CityRemarkBuffer, b2: CityRemarkBuffer): CityRemarkBuffer = {
    val map1: mutable.Map[String, Long] = b1.cityMap
    val map2: mutable.Map[String, Long] = b2.cityMap
    b1.cityMap = map1.foldLeft(map2) {
      case (map, (cityName, count)) => {
        map(cityName) = map.getOrElse(cityName, 0L) + count; map
      }
    }
    b1.totalCount += b2.totalCount
    b1
  }

  override def finish(reduction: CityRemarkBuffer): String = {
    reduction.finish()
  }

  override def bufferEncoder: Encoder[CityRemarkBuffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}