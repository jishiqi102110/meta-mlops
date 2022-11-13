package com.meta.data.pipeline

import com.alibaba.fastjson.JSONObject
import com.meta.Logging
import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.data.conf.HbaseInfoConfig
import com.meta.data.utils.FlowConstant
import com.meta.spark.kafka.{KafkaSource, KafkaSourceStreaming}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._ // scalastyle:ignore
import scala.collection.JavaConverters._ // scalastyle:ignore
import scala.collection.mutable


/**
 * 用来获取item类型特征，有热点属性、重复、无法用过sql达到一次操作的特征
 * 这里将kafka里面的数据，根据配置文件将数据从kafka数据源中读取，然后创造性的将流数据变成批数据，
 * 共享统一的API将数据从kafka落库到Hbase,并且中间还会减少重复数据的读取
 *
 * @author: weitaoliang
 * @version v1.0
 * */
object RedisItemFeature2Hbase extends Logging {

  private val monitorJedis = JedisClusterName.test_cache1

  private var flag = true

  /** 这里将kafka里面的数据，根据配置文件将数据从kafka数据源中读取 */
  def collector(spark: SparkSession,
                config: DataFlowConfigReader,
                kafkaSource: KafkaSourceStreaming,
                sql: String,
                itemColName: String): Unit = {

    logInfo(s"feature collect sql:$sql")
    checkSql(sql)

    // 数据获取
    val ssc = StreamingContext.getActiveOrCreate(
      () => new StreamingContext(spark.sparkContext, kafkaSource.batchDuration))
    val events = kafkaSource.getKafkaDStream(ssc).filter(x => x._2 != null && "".equals(x._2.trim))
    import spark.implicits._ // scalastyle:ignore
    events.foreachRDD {
      (rdd, time) =>
        rdd.toDF("key", "line").createTempView("kafkaTable")
        val df = spark.sql(sql)
        if (flag) {
          checkDF(df)
        }
        df2Hbase(df, config, itemColName)
    }
  }

  /** 根据配置文件从dataFrame获取特征 */
  def df2Hbase(df: DataFrame,
                       config: DataFlowConfigReader,
                       itemColName: String): Unit = {

    val dataFlowDriver = DataFlowDriver(config)
    // sql 中必须存在 requestID、requestTimeStamp、item字段，其他字段默认直接入库
    checkDF(df, itemColName)
    val fieldsNames = df.columns
    df.rdd.mapPartitions {
      rows =>
        rows.map {
          row =>
            val (requestID, rowKey) = getIDAndRowKey(row, config.hbaseConfig)
            (rowKey, row)
        }
    }.groupByKey().mapPartitions { // 这里按照rowKey聚合，入库效率更高
      partitionRecords =>
        // 用partition 级别操作做缓存
        val jedis = JedisConnector(monitorJedis)
        val bufferMap = new mutable.HashMap[String, Any]()
        partitionRecords.map {
          case (rowKey, rows) =>
            val put = new Put(Bytes.toBytes(rowKey))
            rows.foreach {
              row =>
                val requestID = getIDAndRowKey(row, config.hbaseConfig)._2
                val distinctKey = s"${config.hbaseConfig.tableName}_$requestID"
                val item = row.getAs[String]("itemColName")
                // 这里借助redis保证入库hbase时第一次入库，减少重复入库
                if (jedis.hsetnx(distinctKey,
                  s"${itemColName}_$item", "1") == 1L) {
                  // 这里数据过期时间设置为600秒
                  jedis.expire(distinctKey, FlowConstant.HBASE_BUFFER_TIME_OUT)
                  val jsonObj = new JSONObject()
                  for (name <- fieldsNames) {
                    jsonObj.put(name, row.getAs[Any](name))
                  }
                  // 根据配置文件获取特征
                  val map = jsonObj.getInnerMap.asInstanceOf[mutable.HashMap[String, Any]]
                  dataFlowDriver.featureCollector(map, jsonObj, bufferMap)
                  put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(s"${itemColName}_$item"),
                    Bytes.toBytes(jsonObj.toJSONString))
                }
            }
            put
        }
    }.foreachPartitionAsync(config.hbaseConfig.saveData)
  }

  /** 检查入库sql是否规范 */
  private def checkSql(sql: String): Unit = {
    assert(sql.contains(FlowConstant.HBASE_REQUEST_ID_NAME) &
      sql.contains(FlowConstant.HBASE_REQUEST_TIMESTAMP_NAME),
      s"sql 中必须存在 ${FlowConstant.HBASE_REQUEST_ID_NAME}、" +
        s"${FlowConstant.HBASE_REQUEST_TIMESTAMP_NAME}!")
  }


  /** 检查入库dataFrame是否符合规范 */
  private def checkDF(df: DataFrame, itemColName: String) {
    val dfColNames = df.columns
    assert(checkDF(df) & dfColNames.contains(itemColName),
      s"sql 中必须存在 ${FlowConstant.HBASE_REQUEST_ID_NAME}、" +
        s"${FlowConstant.HBASE_REQUEST_TIMESTAMP_NAME}、$itemColName!")
  }

  /** 检查dataFrame是否规范 */
  private def checkDF(df: DataFrame): Boolean = {
    val dfColNames = df.columns
    if (dfColNames.contains(FlowConstant.HBASE_REQUEST_ID_NAME) &&
      dfColNames.contains(FlowConstant.HBASE_REQUEST_TIMESTAMP_NAME)) {
      true
    } else {
      false
    }
  }

  /** 私有方法用来获取rowKey 和请求ID,避免代码重复 */
  private def getIDAndRowKey(row: Row, config: HbaseInfoConfig): (String, String) = {
    val requestID = row.getAs[String](FlowConstant.HBASE_REQUEST_ID_NAME)
    val requestTimeStamp = row.getAs[Long](FlowConstant.HBASE_REQUEST_TIMESTAMP_NAME)
    val rowKey = config.getRowKey(requestID, requestTimeStamp)
    (rowKey, requestID)
  }
}
