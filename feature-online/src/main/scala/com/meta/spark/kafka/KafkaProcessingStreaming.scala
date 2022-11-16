package com.meta.spark.kafka

import com.meta.spark.monitor.SparkMonitor
import com.meta.utils.{CommonConstants, FlowUtils, MLUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DelayDStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * 通用kafka实时处理任务通用类，同步API，所有消费kafka的任务都可使用此类进行实时数据处理，速度没有[[KafkaAsyncProcessingStreaming]]快
 * 此类适合数据更新需要按照前后顺序进行更新，需要获得更新反馈的应用 用户使用只需要传入相关参数调用run()方法即可
 *
 * spark  SparkSession
 * kafkaSource [[KafkaSourceStreaming]] kafka相关配置
 * dataProcessor: RDD[(String, String)] => Unit 用户定义rdd处理方法
 * delayDuration: Duration = Seconds(0)
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class KafkaProcessingStreaming(spark: SparkSession,
                               kafkaSource: KafkaSourceStreaming,
                               dataProcessor: RDD[(String, String)] => Unit,
                               delayDuration: Duration =
                               Seconds(CommonConstants.DEFAULT_BATCH_DURATION)
                              ) extends Serializable {
  final def run(): Unit = {
    val (delayEvents, ssc) = FlowUtils.getEvents(spark, kafkaSource, delayDuration)
    delayEvents.foreachRDD {
      (rdd, time) =>
        dataProcessor(rdd)
        SparkMonitor.synStreamingMonitor(spark, kafkaSource.groupid, time)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
