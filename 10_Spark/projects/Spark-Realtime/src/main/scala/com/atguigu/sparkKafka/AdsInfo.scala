package com.atguigu.sparkKafka

import java.sql.Timestamp

case class AdsInfo(ts: Long,
                   timestamp: Timestamp,
                   dayString: String,
                   hmString: String,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String)
