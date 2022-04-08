package com.meta.feature

import com.meta.conn.tdbank.{TDBanReceiverConfig, TDbankStreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author weitaoliang 
 * @Description:
 *
 */

object SparkStreamingTDbankConsumer {

  val master = "tl-tdbank-tdmanager.tencent-distribute.com:8099"
  val group = "t_ieg_5_b_ieg_o2_rt_cg_yky_algorithm_exposure_factor_etl_1_001" // 消费者组，填写申请的group名
  val topic = "ieg_o2_rt" // 要消费的消息主题
  val tids = Array("yky_algorithm_exposure_factor") // 指定消费的接口id

  def main(args: Array[String]): Unit = {
    /*

      第一步：配置SparkConf

      */
    val sparkConf = new SparkConf().setAppName("TDbank consumer")

    /*

     第二步：创建StreamingContext

     这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心，设置了批次间隔为2秒

     */

    implicit val ssc = new StreamingContext(sparkConf, Seconds(5))

    val tdBankReceiverConfig = new TDBanReceiverConfig()
      .setMaster(master)
      .setGroup(group)
      .setTids(tids)
      .setTopic(topic)
      .setConsumeFromMaxOffset(true)
      .setFilterOnRemote(true) // 开启过滤id
      .setStorageLevel(StorageLevel.MEMORY_AND_DISK)

    val text = new TDbankStreamingContext(ssc).tdBankTextStream(tdBankReceiverConfig, 0)
    ssc.tdBankTextStream(tdBankReceiverConfig, 0)


  }
}
