package com.meta.streaming.spark.kafka

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.FeatureDTO
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import com.meta.featuremeta.{RedisFeatureMeta, RedisSeqListMeta}
import com.meta.streaming.spark.monitor.SparkMonitor
import org.apache.spark.FutureAction
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, DelayDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable

/**
 * 多源序列特征处理类
 * 序列特征说明
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class MultiSequenceEventStreaming(spark: SparkSession,
                                  kafkaSource: KafkaSourceStreaming,
                                  saveJedisClusterName: JedisClusterName,
                                  delayDuration: Duration = Seconds(10)) extends Serializable {

  private val futureActions = new mutable.ArrayBuffer[FutureAction[Long]]()
  private val metaInfoMap = mutable.Map.empty[(String, String), RedisSeqListMeta]

  private val dsTreams = mutable.ArrayBuffer[DStream[Boolean]]()
  val kafkaStream: DStream[(String, String)] = {
    val ssc = StreamingContext.getActiveOrCreate(
      () => new StreamingContext(spark.sparkContext, kafkaSource.batchDuration))
    new DelayDStream(kafkaSource.getKafkaDStream(ssc)).setDelayDuration(delayDuration)
  }

  // events (唯一标志,Array(时间时间，事件标志))
  // 考虑在同一个streaming 中将不同的序列特征存储到不同的redisKey，减少数据消费消耗
  // idFuctionArray  Array(field,通过id获取其他id的方法,保存行为最大长度，行为保存最大时长) 建议保存短期行为，时长不超过2天
  def putEvents(redisKeyPattern: String,
                featureType: FeatureTypeEnum,
                events: DStream[(String, Array[(Long, String)])],
                idFuctionArray: Array[(String, String => String, Int, Int)]
               ): Unit = {
    val dsTreams = this.dsTreams
    val metaInfoMap = this.metaInfoMap
    val saveJedisClusterName = this.saveJedisClusterName
    val intValue2 = FeatureDTO.FieldValue.newBuilder()
      .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(2))
      .build()

    val defaultSeqList = FeatureDTO.SeqList.newBuilder().addVal("").build()
    val seqDefault = FeatureDTO.FieldValue.newBuilder()
      .setValueType(FeatureDTO.FieldValue.ValueType.SeqList)
      .setValue(FeatureDTO.Value.newBuilder.setSeqListVal(defaultSeqList)).build()

    // 填充注册特征元数据
    metaInfoMap ++= idFuctionArray.map {
      case (eventName, _, _, _) =>
        ((redisKeyPattern, eventName),
          new RedisSeqListMeta(saveJedisClusterName, redisKeyPattern,
            eventName, kafkaSource.topics, seqDefault, featureType))
    }

    // 获取过期最大时间
    val ttl = idFuctionArray.map(_._4).max
    val eventName = idFuctionArray.head._1

    dsTreams += events.transform {
      (rdd, time) =>
        rdd.mapPartitions {
          partionRecords =>
            val map = new mutable.HashMap[String, mutable.Map[String, String]]()
            partionRecords.map {
              case (keyID, arr) =>
                for ((eventName, idfun, saveLen, ttl) <- idFuctionArray) {
                  val idMap = map.getOrElseUpdate(eventName, new mutable.HashMap[String, String]())
                  val featureMeta = metaInfoMap((keyID, eventName))
                  val oldValue = featureMeta.get(keyID)
                  val newEvents = arr.sortWith(_._1 > _._1).map {
                    case (timmestamp, eventid) =>
                      (timmestamp, idMap.getOrElseUpdate(eventid, idfun(eventid)))
                  }
                  // combine value
                  //  update newValue
                  featureMeta.save(keyID, null)
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
    val dsTreams = this.dsTreams
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
}
