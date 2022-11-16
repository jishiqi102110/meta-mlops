package com.meta.spark.kafka

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.FeatureDTO
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import com.meta.featuremeta.RedisSeqListMeta
import com.meta.spark.monitor.SparkMonitor
import com.meta.utils.{CommonConstants, MLUtils}
import org.apache.spark.FutureAction
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, DelayDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable

/**
 * 多源序列特征处理类,用户传入相关,用户使用只需要传入相关参数调用run()方法即可
 *
 * spark SparkSession
 * kafkaSource [[KafkaSourceStreaming]] kafka相关配置
 * saveJedisClusterName 存储集群名称
 * delayDuration stream允许延迟时间，默认10s
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class MultiSequenceEventStreaming(spark: SparkSession,
                                  kafkaSource: KafkaSourceStreaming,
                                  saveJedisClusterName: JedisClusterName,
                                  delayDuration: Duration = Seconds(
                                    CommonConstants.DEFAULT_DELAY_DURATION)
                                 ) extends Serializable {

  private val futureActions = new mutable.ArrayBuffer[FutureAction[Long]]()
  private val metaInfoMap = mutable.Map.empty[(String, String), RedisSeqListMeta]

  private val dsDreams = mutable.ArrayBuffer[DStream[Boolean]]()
  // 获取kafka消息数据流，这里设置了延迟时间，默认10s
  val kafkaStream: DStream[(String, String)] = {
    val ssc = MLUtils.getSparkStreaming(spark, kafkaSource.batchDuration)
    new DelayDStream(kafkaSource.getKafkaDStream(ssc)).setDelayDuration(delayDuration)
  }


  /**
   * 考虑在同一个streaming 中将不同的序列特征存储到不同的redisKey，减少数据消费消耗
   *
   * @Param redisKeyPattern 特征存储key
   * @Param featureType 特征存储类型
   * @Param events (唯一标志,Array(时间时间，事件标志))
   * @Param idFunctionArray Array(field,通过id获取其他id的方法,保存行为最大长度，行为保存最大时长) 建议保存短期行为，时长不超过2天
   */

  def putEvents(redisKeyPattern: String,
                featureType: FeatureTypeEnum,
                events: DStream[(String, Array[(Long, String)])],
                idFunctionArray: Array[(String, String => String, Int, Int)]
               ): Unit = {

    fillMetaInfoMap(redisKeyPattern, featureType, idFunctionArray)
    // 获取过期最大时间
    val ttl = idFunctionArray.map(_._4).max
    val eventName = idFunctionArray.head._1
    dsDreams += events.transform {
      (rdd, time) =>
        rdd.mapPartitions {
          partitionRecords =>
            val map = new mutable.HashMap[String, mutable.Map[String, String]]()
            partitionRecords.map {
              case (keyID, arr) =>
                for ((eventName, idFun, saveLen, ttl) <- idFunctionArray) {
                  val idMap = map.getOrElseUpdate(eventName, new mutable.HashMap[String, String]())
                  val featureMeta = metaInfoMap((keyID, eventName))
                  val oldValue = featureMeta.get(keyID)
                  val newEvents = arr.sortWith(_._1 > _._1).map {
                    case (timestamp, eventId) =>
                      (timestamp, idMap.getOrElseUpdate(eventId, idFun(eventId)))
                  }
                  // combine value
                  //  update newValue
                  featureMeta.save(keyID, null) // scalastyle:ignore
                }
                // 这里统一设置过期时间
                val meta = metaInfoMap(redisKeyPattern, eventName)
                meta.expire(keyID, ttl)
                true
            }
        }
    }
  }

  def run(): Unit = {
    val dsTreams = this.dsDreams
    val kafkaStream = this.kafkaStream
    val ssc = kafkaStream.context
    ssc.union(dsTreams).foreachRDD {
      (rdd, time) =>
        futureActions += rdd.countAsync()
        SparkMonitor.synStreamingMonitor(spark, kafkaSource.groupid, time)
    }
    // 特征注册
    metaInfoMap.values.foreach(_.register())
    ssc.start()
    ssc.awaitTermination()
  }

  /** 填充注册特征元数据 */
  private def fillMetaInfoMap(redisKeyPattern: String,
                              featureType: FeatureTypeEnum,
                              idFunctionArray: Array[(String, String => String, Int, Int)]
                             ): Unit = {
    //    val intValue2 = FeatureDTO.FieldValue.newBuilder()
    //      .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
    //      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(2))
    //      .build()

    val defaultSeqList = FeatureDTO.SeqList.newBuilder().addVal("").build()
    val seqDefault = FeatureDTO.FieldValue.newBuilder()
      .setValueType(FeatureDTO.FieldValue.ValueType.SeqList)
      .setValue(FeatureDTO.Value.newBuilder.setSeqListVal(defaultSeqList)).build()

    // 填充注册特征元数据
    metaInfoMap ++= idFunctionArray.map {
      case (eventName, _, _, _) =>
        ((redisKeyPattern, eventName),
          new RedisSeqListMeta(saveJedisClusterName, redisKeyPattern,
            eventName, kafkaSource.topics, seqDefault, featureType))
    }
  }
}
