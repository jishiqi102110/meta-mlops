package com.meta.utils

import com.meta.spark.kafka.KafkaSourceStreaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, DelayDStream}

/**
 * Feature-flow工具类
 *
 * @author: weitaoliang
 * @version v1.0
 * */
object FlowUtils {

  /** 根据ssc、kafkaSource 获取stream */
  def getKafkaStream(ssc: StreamingContext,
                     kafkaSourceStreaming: KafkaSourceStreaming): DStream[(String, String)] = {
    kafkaSourceStreaming.getKafkaDStream(ssc)
  }

  /** 根据spark、kafkaSourceStreaming 获取stream */
  def getEvents(spark: SparkSession,
                kafkaSourceStreaming: KafkaSourceStreaming,
                delayDuration: Duration = Seconds(0)
               ): (DStream[(String, String)], StreamingContext) = {
    val ssc = MLUtils.getSparkStreaming(spark, kafkaSourceStreaming.batchDuration)
    val kafkaStream = getKafkaStream(ssc, kafkaSourceStreaming)
    val events = kafkaStream.filter(x => x._2 != null && x._2.trim != "")
    val delayEvents = if (delayDuration == Seconds(0)) {
      events
    } else {
      new DelayDStream(events).setDelayDuration(delayDuration)
    }
    (delayEvents, ssc)
  }
}
