package com.meta.spark.monitor

import com.alibaba.fastjson.JSONObject
import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.featuremeta.{RedisFeatureInfo, RedisFeatureMeta}
import org.apache.spark.FutureAction
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Time

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONArray
import sys.process._

/**
 * spark程序监控，输出格式化监控信息到redis,用于监控spark异步任务和同步任务
 *
 * @author: weitaoliang
 * @version v1.0
 * */
object SparkMonitor {

  private lazy val monitorJedis = JedisConnector(JedisClusterName.test_cache1)

  private var _yarnTrackingUrl: String = _

  def getYarnTrackingUrl(spark: SparkSession): String = {
    if (_yarnTrackingUrl == null) {
      val rmids = spark.sparkContext.hadoopConfiguration.get("yarn.resourcemanager.ha.rm-ids")
      val applicationID = spark.sparkContext.applicationId
      val conf = if (rmids != null) {
        val address = spark.sparkContext.hadoopConfiguration.
          get(s"yarn.resourcemanager.webapp.address.${rmids.split(",").head}")
        s"http://$address/conf"
      } else {
        null
      }
      // 这里执行系统命令
      val curResult = s"curl ${conf}".!!
      // 每个公司可能获取spark任务链接地址的方式不一样
      _yarnTrackingUrl = curResult.toString
    }
    _yarnTrackingUrl
  }

  // 用于异步sparkStreaming 监控信息到redis

  def asyStreamingMonitor[T](spark: SparkSession,
                             groupID: String,
                             futureActions: ArrayBuffer[FutureAction[T]]
                            ): Unit = {
    futureActions --= futureActions.filter(_.isCompleted)
    val json = new JSONObject()
    val appName = spark.sparkContext.appName
    val application_id = spark.sparkContext.applicationId
    json.put("tracking_url", getYarnTrackingUrl(spark))
    json.put("application_id", application_id)
    json.put("uncompleted_num", futureActions.size)
    json.put("app_name", appName)
    json.put("user", spark.sparkContext.sparkUser)
    json.put("timeStamp", System.currentTimeMillis())
    json.put("groupid", groupID)
    monitorJedis.hset("asyn_streaming_monitor", application_id + "|" + appName, json.toJSONString)
  }

  def synStreamingMonitor(spark: SparkSession,
                          groupID: String,
                          time: Time
                         ): Unit = {
    val json = new JSONObject()
    val appName = spark.sparkContext.appName
    val application_id = spark.sparkContext.applicationId
    json.put("tracking_url", getYarnTrackingUrl(spark))
    json.put("application_id", application_id)
    // 可以看到任务堆积多久
    json.put("batch_time", time.milliseconds)
    json.put("app_name", appName)
    json.put("user", spark.sparkContext.sparkUser)
    json.put("timeStamp", System.currentTimeMillis())
    json.put("groupid", groupID)
    monitorJedis.hset("syn_streaming_monitor", application_id + "|" + appName, json.toJSONString)
  }

  // 用于监控离线任务执行时长
  def runningTimeMonitor[T](spark: SparkSession, f: () => T): Unit = {
    val startTimeStamp = System.currentTimeMillis()
    val num = f() match {
      case i: Int => i
      case l: Long => l
      case d: Double => d
      case f: Float => f
      case _ => 0
    }
    val endTimeStamp = System.currentTimeMillis()
    val json = new JSONObject()
    val appName = spark.sparkContext.appName
    val application_id = spark.sparkContext.applicationId
    json.put("tracking_url", getYarnTrackingUrl(spark))
    json.put("application_id", application_id)
    // 更新数量
    json.put("num", num)
    json.put("app_name", appName)
    json.put("user", spark.sparkContext.sparkUser)
    json.put("startTimeStamp", startTimeStamp)
    json.put("endTimeStamp", endTimeStamp)

    monitorJedis.hset("runningTimeMonitor", application_id + "|" + appName, json.toJSONString)
  }

  // 离线特征入库往往是一批特征对应一个任务，所以这的信息也进行一下整合
  def offlineFeatureUpdateMonitor(spark: SparkSession,
                                  startTimeStamp: Long,
                                  endTimeStamp: Long,
                                  metaInfos: Array[RedisFeatureInfo],
                                  updateNums: Array[Int],
                                  maxValues: Array[Double],
                                  minValues: Array[Double]): Unit = {
    val json = new JSONObject()
    val appName = spark.sparkContext.appName
    val application_id = spark.sparkContext.applicationId
    json.put("tracking_url", getYarnTrackingUrl(spark))
    json.put("application_id", application_id)
    // 更新数量
    json.put("app_name", appName)
    json.put("user", spark.sparkContext.sparkUser)
    json.put("startTimeStamp", startTimeStamp)
    json.put("endTimeStamp", endTimeStamp)

    val dataArray = new mutable.ArrayBuffer[JSONObject]()
    for (i <- 0 to metaInfos.size - 1) {
      val metaJson = new JSONObject
      val meta = metaInfos(i)
      metaJson.put("updateNum", updateNums(i))
      metaJson.put("maxValue", maxValues(i))
      metaJson.put("minValue", minValues(i))

      metaJson.put("featureKey", meta.redisKeyPattern)
      metaJson.put("featureName", meta.redisField)
      metaJson.put("jedisClusterName", meta.jedisClusterName)
      metaJson.put("dataSource", meta.dataSource)
      dataArray ++= dataArray
    }
    val jsonArray = new JSONArray(dataArray.toList)
    json.put("metaArray", jsonArray)

    monitorJedis.hset("offline_feature_update_monitor",
      application_id + "|" + appName, json.toJSONString)
  }

}
