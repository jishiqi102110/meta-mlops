package com.meta.utils

import com.meta.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._ // scalastyle:ignore

/**
 * streamingUtils 用来产生kafka stream
 *
 * 很多业务都是借助zookeeper或者实时计算平台来帮助记录offset，也有业务使用mysql来记录，这里提供一种，使用redis来记录offset方法，会比zookeeper
 * 更加稳定
 * 使用redis记录kafka 消费offsets，用户可以提供redis用来记录kafka的 offset,如果redis不存在则就按照用户kafka消费参数消费
 * 如果提供则会在每次消费的时候记录最新的offset,然后在消费的时候从最新的offset进行消费
 *
 * @author: weitaoliang
 * @version v1.0
 * */
object StreamingUtils extends Logging {

  private final val ConsumerConfig_DESERIALIZER_CLASS_CONFIG =
    "org.apache.kafka.common.serialization.StringSerializer"

  // 初始化kafka消费参数
  private def initKafkaParams(topics: String,
                              broker: String,
                              groupID: String): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        ConsumerConfig_DESERIALIZER_CLASS_CONFIG,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        ConsumerConfig_DESERIALIZER_CLASS_CONFIG,
      ConsumerConfig.GROUP_ID_CONFIG -> groupID,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> java.lang.Boolean.FALSE)
  }

  // 提交kafka消费记录到redis
  private def sysSubmitKafkaOffsets(offsetRanges: Array[OffsetRange],
                                    consumersOffsetKey: String,
                                    jedis: Jedis): Unit = {
    offsetRanges.map {
      o =>
        (o.topic, s"${o.partition}:${o.fromOffset}")
    }.groupBy(_._1).mapValues(_.map(_._2)).foreach {
      case (topic, offsets) =>
        jedis.hset(consumersOffsetKey, topic, offsets.mkString(","))
        // 过期时间设置三天，如果保留太长日志太多堆积也处理不过来
        jedis.expire(consumersOffsetKey, 24 * 3 * 60 * 60)
    }
    for (o <- offsetRanges) {
      logInfo(
        s"########################" +
          s" submit topic:${o.topic} " +
          s"partition:${o.partition} " +
          s"fromOffset:${o.fromOffset}" +
          s"untilOffset:${o.untilOffset}" +
          s" ###########################")
    }
  }

  // 填充fromOffset
  private def fillFromOffset(topics: String,
                             consumersOffsetKey: String,
                             jedis: Option[Jedis]): Map[TopicPartition, java.lang.Long] = {
    var fromOffsets: Map[TopicPartition, java.lang.Long] = Map()
    topics.split(",").foreach {
      topic =>
        val consumersOffsetStr = jedis.get.hget(consumersOffsetKey, topic)
        if (consumersOffsetStr != null) {
          val consumersOffset = consumersOffsetStr.split(",").map {
            offset =>
              offset.split(":")
          }.map(o => (o(0).toInt, o(1).toLong))

          for ((i, offset) <- consumersOffset) {
            val topicPartition = new TopicPartition(topic, i)
            // 将不同partition 对应的offset 增加到fromOffsets中
            fromOffsets += (topicPartition -> offset)
            logDebug("#############" +
              s"add topic :$topic partition:$topicPartition offset:$offset")
          }
        }
    }
    fromOffsets
  }

  // 获取stream从redis获取
  private def createStreamWithoutRedis(topics: String,
                                       broker: String,
                                       groupID: String,
                                       ssc: StreamingContext,
                                       kafkaParams: Map[String, Object]
                                      ): Option[DStream[ConsumerRecord[String, String]]] = {

    val topicSet: Set[String] = topics.split(",").map(_.trim).toSet
    // 如果不配置redis，则直接按照用户指定参数消费即可
    val directKafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topicSet, kafkaParams))
    val kafkaStream = Some(directKafkaStream)
    kafkaStream.get.foreachRDD {
      (rdd, time) =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        kafkaStream.get.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        logInfo("##################" +
          s"${time.milliseconds} offset" +
          "#########################")
        offsetRanges.foreach {
          offsetRange =>
            logDebug("#################" +
              s"topic:${offsetRange.topic}" +
              s"partion:${offsetRange.partition}" +
              s"fromOffset:${offsetRange.fromOffset}" +
              s" untilOffset:${offsetRange.untilOffset}" +
              "#######################")
        }
    }
    kafkaStream
  }

  // 获取stream从redis获取
  private def createStreamWithRedis(topics: String,
                                    broker: String,
                                    groupID: String,
                                    jedis: Option[Jedis],
                                    ssc: StreamingContext,
                                    kafkaParams: Map[String, Object]
                                   ): Option[DStream[ConsumerRecord[String, String]]] = {

    val topicSet: Set[String] = topics.split(",").map(_.trim).toSet
    // 获取redis 消费的key
    val consumersOffsetKey = s"kafka-consumers-$groupID-offsets"
    logInfo(
      "#####################" +
        s"acquire kafka consumer key :$consumersOffsetKey" +
        "#####################"
    )
    val fromOffsets: Map[TopicPartition, java.lang.Long]
    = fillFromOffset(topics, consumersOffsetKey, jedis)

    val directKafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topicSet, kafkaParams, fromOffsets))
    val kafkaStream = Some(directKafkaStream)
    // 这里提交offset保存到redis
    kafkaStream.get.foreachRDD {
      rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        sysSubmitKafkaOffsets(offsetRanges, consumersOffsetKey, jedis.get)
    }
    kafkaStream
  }

  // 获取kafkaStream方法
  def getKafkaStream(topics: String,
                     broker: String,
                     groupID: String,
                     jedis: Option[Jedis],
                     ssc: StreamingContext,
                     consumerParams: Map[String, String]
                    ): DStream[(String, String)] = {

    var kafkaStream: Option[DStream[ConsumerRecord[String, String]]] = None
    // 初始化kafka消费参数
    val kafkaParams = initKafkaParams(topics, broker, groupID) ++ consumerParams
    if (!jedis.isEmpty) {
      // 如果用户传入redis则说明需要记录offset
      kafkaStream = createStreamWithRedis(topics, broker, groupID, jedis, ssc, kafkaParams)
    }
    else {
      // 如果用户不传入redis则说明不需要记录消费offset
      kafkaStream = createStreamWithoutRedis(topics, broker, groupID, ssc, kafkaParams)
    }
    kafkaStream.get.map {
      record =>
        (record.topic(), record.value())
    }
  }
}
