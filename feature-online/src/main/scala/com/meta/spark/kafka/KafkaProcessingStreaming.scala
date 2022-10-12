package com.meta.spark.kafka

import com.meta.spark.monitor.SparkMonitor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DelayDStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * kafka 通用处理方法父类
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class KafkaProcessingStreaming(spark: SparkSession,
                               kafkaSource: KafkaSourceStreaming,
                               dataProcessor: RDD[(String, String)] => Unit,
                               delayDuration: Duration = Seconds(0)
                              ) extends Serializable {
  final def run(): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(
      () => new StreamingContext(spark.sparkContext, kafkaSource.batchDuration))
    val kafkaStream = kafkaSource.getKafkaDStream(ssc)
    val events = kafkaStream.filter(x => x._2 != null && x._2.trim != "")
    val delayEvents = if (delayDuration == Seconds(0)) {
      events
    } else {
      new DelayDStream(events).setDelayDuration(delayDuration)
    }

    delayEvents.foreachRDD {
      (rdd, time) =>
        dataProcessor(rdd)
        SparkMonitor.synStreamingMonitor(spark, kafkaSource.groupid, time)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
