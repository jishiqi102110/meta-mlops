package com.meta.feature

import com.meta.conn.tdbank.TDBanReceiverConfig
import com.tencent.tdw.spark.toolkit.tdbank.TDBankProvider
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.tencent.tdw.spark.toolkit.tdbank.{ReceiverConfig, TubeReceiverConfig}
import org.apache.spark.storage.StorageLevel


/**
* 写一个spark消费TDbank的测试类
*
*/

object SparkStreamingTDbank {

  val master= "tl-tdbank-tdmanager.tencent-distribute.com:8099"
  val group = "t_ieg_5_b_ieg_o2_rt_cg_yky_algorithm_exposure_factor_etl_1_001"  // 消费者组，填写申请的group名
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


    val ssc = new StreamingContext(sparkConf,Seconds(5))

    /**

     * 接口需要tid数据 interfaceid

     * 消费tube的数据，要是master地址，topic、消费组

     * tube还没过滤数据的功能 写填写tid = null

     */

    val tdbank = new TDBankProvider(ssc)

    val receiverConfig = new TubeReceiverConfig()
      .setMaster(master)
      .setGroup(group)
      .setTids(tids)
      .setTopic(topic)
      .setConsumeFromMaxOffset(true)
      .setFilterOnRemote(true) // 开启过滤id
      .setStorageLevel(StorageLevel.MEMORY_AND_DISK)


    val textStream = tdbank.textStream(receiverConfig,10)

    textStream.foreachRDD{
      rdd=>
        rdd.foreachPartition{
          partion=>
            partion.foreach{
              line=>
                println(s"textStream  $line")
            }
        }
    }

    ssc.start()

    ssc.awaitTermination()
  }

}
