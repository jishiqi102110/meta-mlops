package com.meta.spark.kafka

import com.meta.spark.monitor.SparkMonitor
import com.meta.utils.{FlowUtils, MLUtils}
import org.apache.spark.FutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.dstream.DelayDStream

/**
 * kafka异步处理工具类,可以支持异步实时处理任务，处理速度更快，里面加载了任务监控组件，可以用来监控任务堆积情况
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class KafkaAsyncProcessingStreaming(spark: SparkSession,
                                    kafkaSource: KafkaSourceStreaming,
                                    dataAsynProcess: RDD[(String, String)] => FutureAction[Unit],
                                    delayDuration: Duration = Seconds(0)
                                   ) extends Serializable {
  // 监控需要，可以用来监控堆积了多少任务
  private val futureActions = new ArrayBuffer[FutureAction[Unit]]

  final def run(): Unit = {
    val (delayEvents, ssc) = FlowUtils.getEvents(spark, kafkaSource, delayDuration)
    delayEvents.foreachRDD {
      (rdd, time) =>
        val futureAction = dataAsynProcess(rdd)
        futureActions += futureAction
        // 这里提交监控信息
        SparkMonitor.asyStreamingMonitor(spark, kafkaSource.groupid, futureActions)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
