package com.meta.sys

import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import com.meta.featuremeta.{RedisFeatureInfo, RedisIntMeta}
import com.sun.org.slf4j.internal.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util
import scala.collection.mutable
import scala.util.Random
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._ //scalastyle:ignore

/**
 * 离线特征入库工具类
 *
 * @author weitaoliang
 * @version V1.0
 * @date 2021/9/30 16:35
 * */


object Hive2RedisUtils {
  private val logger = LoggerFactory.getLogger(Hive2RedisUtils.getClass)
  private final val CHAR_SET_NAME = "UTF-8"

  def runSql(spark: SparkSession,
             sql: String,
             jedisClusterName: JedisClusterName,
             redisKeyPattern: String,
             dataSource: String,
             defaultValue: Map[String, Any],
             batch_size: Int,
             redisTTL: Int = 60 * 24 * 60 * 60
            ): Unit = {

    val intValue = FeatureDTO.FieldValue.newBuilder()
      .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(0))
      .build()

    val keyplaceHolder = new RedisIntMeta(jedisClusterName, redisKeyPattern, "",
      dataSource, intValue, FeatureTypeEnum.USER).keyPlaceHolder.get

    val _featureMetasAndGetFeatureMethods = SparkMetaUtils.schemaMetas(
      spark.sql(sql).drop(colName = keyplaceHolder),
      redisKeyPattern,
      jedisClusterName,
      defaultValue,
      dataSource)

    runHMset(spark, spark => spark.sql(sql),
      row => {
        row.getAs[String](fieldName = keyplaceHolder)
      },
      _featureMetasAndGetFeatureMethods,
      jedisClusterName,
      batch_size,
      redisTTL
    )
  }

  def runHMset(spark: SparkSession,
               generateDF: SparkSession => DataFrame,
               getIDFromRow: Row => String,
               featureMetasAndGetFeatureMethods: Seq[(RedisFeatureInfo, Row => Array[Byte])],
               jedisClusterName: JedisClusterName,
               batch_size: Int,
               redisTTL: Int = 60 * 24 * 60 * 60
              ): Unit = {
    import spark.implicits._ //scalastyle:ignore

    val startTimeStamp = System.currentTimeMillis()

    assert(featureMetasAndGetFeatureMethods.map(
      x => (x._1.redisKeyPattern, x._1.jedisClusterName))
      .distinct.length == 1, "所有field的key必须是同一个！！")

    val keyAndFields = generateFeatureBytes(spark, generateDF, getIDFromRow,
      featureMetasAndGetFeatureMethods)
    setToRedis(keyAndFields, jedisClusterName, batch_size, redisTTL)
    val endTimeStamp = System.currentTimeMillis()

    // scalastyle:off println
    println("入库时间 : " + (endTimeStamp - startTimeStamp) / 1000 / 60 + " min.")
    // scalastyle:on println
    featureMonitor(featureMetasAndGetFeatureMethods)
  }

  private def generateFeatureBytes(spark: SparkSession,
                                   generateDF: SparkSession => DataFrame,
                                   getIDFromRow: Row => String,
                                   featureMetasAndGetFeatureMethods:
                                   Seq[(RedisFeatureInfo, Row => Array[Byte])])
  : RDD[Option[(String, mutable.Map[Array[Byte], Array[Byte]])]] = {
    import spark.implicits._ // scalastyle:ignore
    val fieldUpdateNum = generateDF(spark).rdd.map {
      row =>
        val id = getIDFromRow(row)
        if (id != null) {
          val featureInfoHead = featureMetasAndGetFeatureMethods.head._1
          val redisKey = featureInfoHead.getKey(id)
          val fieldMap = new java.util.HashMap[Array[Byte], Array[Byte]]()
          for ((featureInfo, getFeature) <- featureMetasAndGetFeatureMethods) {
            val value = getFeature(row)
            if (value != null) {
              fieldMap.put(featureInfo.redisField.getBytes(CHAR_SET_NAME), value)
            }
          }
          Some(redisKey, fieldMap.asScala)
        } else {
          None
        }
    }.filter(!_.isEmpty)
    fieldUpdateNum
  }

  private def featureMonitor(
                              featureMetasAndGetFeatureMethods: Seq[(RedisFeatureInfo, Row => Array[Byte])]): Unit = {
    featureMetasAndGetFeatureMethods.map(_._1).foreach {
      featureInfo =>
        featureInfo.register()
      // 这里可以写一个监控组件，把更新信息写入到redis或者其他数据库
    }
  }

  private def setToRedis(
                          keyAndFields:
                          RDD[Option[(String, mutable.Map[Array[Byte], Array[Byte]])]],
                          jedisClusterName: JedisClusterName,
                          batch_size: Int,
                          redisTTL: Int): Unit = {
    keyAndFields.foreachPartition {
      partition =>
        val jedis = JedisConnector(jedisClusterName)
        val dataList = partition.toArray
        // 这里将数据划分为多个段，每个段batch_size 个记录进行redis的操作，采用pipeline 形式入库
        val nStep = math.ceil(dataList.size / batch_size.toDouble).toInt

        for (index <- 0 to nStep) {
          val lowerIndex = batch_size * index
          val upperIndex = if (lowerIndex + batch_size >= dataList.size) {
            dataList.size
          }
          else {
            batch_size * (index + 1)
          }
          val batchData = dataList.slice(lowerIndex, upperIndex)
          val pipeline = jedis.pipelined()
          batchData.map(_.get).foreach {
            case (redisKey, jMap) =>
              if (Random.nextDouble() <= 0.00001) {
                // scalastyle:off println
                println("入库redis中,redisKey is " + redisKey + ", ttl is " + redisTTL)
                // scalastyle:on println
              }
              pipeline.hmset(redisKey.getBytes(CHAR_SET_NAME), jMap)
              pipeline.expire(redisKey, redisTTL)
          }
          pipeline.sync()
        }
        jedis.close()
    }
  }
}