package com.meta.spark.kafka

import com.meta.spark.monitor.SparkMonitor
import com.meta.utils.{CommonConstants, FlowUtils, MLUtils}
import org.apache.spark.FutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.dstream.DelayDStream

/**
 * kafka数据源异步处理通用类,可以支持异步实时处理任务，处理速度更快，里面加载了任务监控组件，可以用来监控任务堆积情况，所有消费kafka的任务都可使用
 * 此类进行实时数据处理；注意此种处理方法如果任务堆积spark是不会进行告警的，所以需要自己使用我们自定义的监控系统来进行异步任务监控,用户使用只需要传入相关参数调用run()方法即可
 *
 * spark  SparkSession
 * kafkaSource [[KafkaSourceStreaming]] kafka相关配置类
 * dataAsyncProcess  用户传入的RDD[(String, String)] 转化为 FutureAction[Unit] 的方法，此为自定义代码块
 * delayDuration 用户允许的实时消息处理的延迟时间，我们默认是不延迟，但是也支持延迟处理，来保证处理速度，因为有的业务可能不需要那么高的实时处理
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class KafkaAsyncProcessingStreaming(spark: SparkSession,
                                    kafkaSource: KafkaSourceStreaming,
                                    dataAsyncProcess: RDD[(String, String)] => FutureAction[Unit],
                                    delayDuration: Duration =
                                    Seconds(CommonConstants.DEFAULT_BATCH_DURATION)
                                   ) extends Serializable {
  // 监控需要，可以用来监控堆积了多少任务
  private val futureActions = new ArrayBuffer[FutureAction[Unit]]

  final def run(): Unit = {
    val (delayEvents, ssc) = FlowUtils.getEvents(spark, kafkaSource, delayDuration)
    delayEvents.foreachRDD {
      (rdd, time) =>
        val futureAction = dataAsyncProcess(rdd)
        futureActions += futureAction
        // 这里提交监控信息
        SparkMonitor.asyStreamingMonitor(spark, kafkaSource.groupid, futureActions)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
