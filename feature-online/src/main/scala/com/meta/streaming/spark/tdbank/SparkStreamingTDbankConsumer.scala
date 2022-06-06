package com.meta.feature

import com.meta.conn.tdbank.TDbankStreamingContext.fromStreamingContext
import com.meta.conn.tdbank.{TDBanReceiverConfig}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author weitaoliang 
 *
 */

object SparkStreamingTDbankConsumer {

  private final val master = "tl-tdbank-tdmanager.tencent-distribute.com:8099"

  // 消费者组，填写申请的group名
  private final val group = "t_ieg_5_b_ieg_o2_rt_cg_yky_algorithm_exposure_factor_etl_1_001"
  private final val topic = "ieg_o2_rt" // 要消费的消息主题
  private final val tids = Array("yky_algorithm_exposure_factor") // 指定消费的接口id
  private final val DURATION = 5

  def main(args: Array[String]): Unit = {

    /*
      第一步：配置SparkConf
      */
    val sparkConf = new SparkConf().setAppName("TDbank consumer")

    /*

     第二步：创建StreamingContext

     这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心，设置了批次间隔为2秒

     */

    implicit val ssc = new StreamingContext(sparkConf, Seconds(DURATION))

    val numExecutors = sparkConf.getInt("spark.executor.instances", 0)


    val tdBankReceiverConfig = new TDBanReceiverConfig()
      .setMaster(master)
      .setGroup(group)
      .setTids(tids)
      .setTopic(topic)
      .setConsumeFromMaxOffset(true)
      .setFilterOnRemote(true) // 开启过滤id
      .setStorageLevel(StorageLevel.MEMORY_AND_DISK)

    // 这里就可以使用 TDbankStreamingContext 里的伴生对象的隐式函数，把StreamingContext 转化成  TDbankStreamingContext
    // 直接使用ssc调用 tdBankTextStream方法
    val textStream = ssc.tdBankTextStream(tdBankReceiverConfig, numExecutors)

    textStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          partion =>
            partion.foreach {
              line =>
                println(s"textStream  $line")
            }
        }
    }

    ssc.start()

    ssc.awaitTermination()

  }
}
