package com.meta.sys

import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import com.meta.featuremeta.{RedisFeatureInfo, RedisFeatureMeta, RedisIntMeta}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import java.util

import com.meta.Logging

import scala.collection.mutable
import scala.util.Random
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._ //scalastyle:ignore

/**
 * 离线特征入库工具类,帮助用户使用sql就可以完成特征的注册及入库，只需要调用 runSql函数即可，此方法是特征平台离线特征入库平台的底层SDK
 *
 * @author weitaoliang
 * @version V1.0
 * */


object Hive2RedisUtils extends Logging {
  private final val CHAR_SET_NAME = "UTF-8"

  /**
   * 特征入库主要方法，用户只需要传入类似hive sql 进行取数即可，但是注意的是，sql必须包含redisKeyPattern里面的填充符（keyplaceHolder）,
   * 例如device_id
   *
   * @Param [spark] sparkSession对象
   * @Param [sql] 用户sql egg: select device_id,age,sex,netType from table_test where xxx,其中sql中必须包含 redisKeyPattern
   *        里面的填充符（keyPlaceHolder）
   * @Param [jedisClusterName] 封装的特征存储集群
   * @Param [redisKeyPattern] 特征存储key egg:user_commonKey:{device_id}
   * @Param [dataSource] 数据源字符串，用来记录特征血缘,这里就是 table_test
   * @Param [defaultValue] 特征默认值，这样避免默认值刷入redis,减少特征存储
   * @Param [batch_size] 这里传入写入redis pipeline模式一次性提交多少命令，默认20,减少redis IO压力,提高写入性能
   * @Param [redisTTL] 特征存储的过期时间,默认60天
   */
  def runSql(spark: SparkSession,
             sql: String,
             jedisClusterName: JedisClusterName,
             redisKeyPattern: String,
             dataSource: String,
             defaultValue: Map[String, Any],
             batch_size: Int,
             redisTTL: Int = 60 * 24 * 60 * 60
            ): Unit = {


    // 这里随机构造一个特征主要是利用meta基类方法去拿填充符（keyPlaceHolder），从而在dataFrame中去掉这一列
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


  /**
   * 特征入库hmset方法
   *
   * @Param [spark] sparkSession对象
   * @Param [generateDF] 将spark对象转换为DF的方法
   * @Param [getIDFromRow] 获取key填充符的方法
   * @Param [featureMetasAndGetFeatureMethods] 特征元数据信息及将row转化为特征序列化值的方法数组
   * @Param [jedisClusterName] 封装的特征存储集群
   * @Param [batch_size] 这里传入写入redis pipeline模式一次性提交多少命令，默认20,减少redis IO压力,提高写入性能
   * @Param [redisTTL] 特征存储的过期时间,默认60天
   */
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

    // 检测是否有异常特征
    assert(featureMetasAndGetFeatureMethods.map(
      x => (x._1.redisKeyPattern, x._1.jedisClusterName))
      .distinct.length == 1, "所有field的key必须是同一个！！")

    val keyAndFields = generateFeatureBytes(spark, generateDF, getIDFromRow,
      featureMetasAndGetFeatureMethods)

    setToRedis(keyAndFields, jedisClusterName, batch_size, redisTTL)
    val endTimeStamp = System.currentTimeMillis()
    val redisKey = featureMetasAndGetFeatureMethods.head._1.redisKeyPattern
    val fields = featureMetasAndGetFeatureMethods.map(_._1.redisField).toArray.mkString(",")

    logInfo("***********************************************")
    logInfo(s"key:$redisKey fields:$fields 入库时间 : " +
      (endTimeStamp - startTimeStamp) / 1000 / 60 + " min.")
    logInfo("***********************************************")
    featureMonitor(featureMetasAndGetFeatureMethods)
  }

  /**
   * 生成特征序列化结果方法
   *
   * @Param [spark]  sparkSession对象
   * @Param [generateDF] 将spark对象转换为DF的方法
   * @Param [getIDFromRow] 获取key填充符的方法
   * @Param [featureMetasAndGetFeatureMethods] 特征元数据信息及将row转化为特征序列化值的方法数组
   * @return RDD[Option[(String, mutable.Map[Array[Byte], Array[Byte]])]]
   */
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

  private def featureMonitor(featureMetasAndGetFeatureMethods:
                             Seq[(RedisFeatureInfo, Row => Array[Byte])]): Unit = {
    featureMetasAndGetFeatureMethods.map(_._1).foreach {
      featureInfo =>
        featureInfo.register()
      // 这里加入监控组件，把更新信息写入到redis或者其他数据库
    }
  }

  /**
   * 入库redis方法,采用pipeline异步接口模式，合并请求减少客户端与redis网络开销
   *
   * @Param [keyAndFields] 特征key及对应hash数据bytesmap
   * @Param [jedisClusterName] 封装的特征存储集群
   * @Param [batch_size] 这里传入写入redis pipeline模式一次性提交多少命令，默认20,减少redis IO压力,提高写入性能
   * @Param [redisTTL] 特征存储的过期时间,默认60天
   */
  private def setToRedis(keyAndFields:
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
              // 采用随机打印方式查看日志
              if (Random.nextDouble() <= 0.00001) {
                logInfo("***********************************************")
                logInfo("入库redis中,redisKey is " + redisKey + ", ttl is " + redisTTL)
                logInfo("***********************************************")
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