package com.meta.spark.kafka

import com.meta.conn.kafka.KafkaConnector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.FutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 将数据从一个kafka发送到另外一个kafka通用类,用户使用只需要传入相关参数调用run()方法即可
 *
 * spark SparkSession
 * kafkaSource [[KafkaSourceStreaming]] kafka相关配置类
 * outBrokers 输出的brokers信息
 * outTopic 输出得到topic
 * rddTransformer  RDD[(String, String)] => RDD[String] 自定义用户 RDD[(String, String)] 转化为 RDD[String]方法，用户自定义模块
 * producerParams 生产者参数设置
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class Kafka2KafkaStreaming(spark: SparkSession,
                           kafkaSource: KafkaSourceStreaming,
                           outBrokers: String,
                           outTopic: String,
                           rddTransformer: RDD[(String, String)] => RDD[String],
                           producerParams: Map[String, String] = Map.empty[String, String]
                          ) extends Serializable {

  // kafka消息异步发射处理器
  private def dataAsyncProcess(events: RDD[(String, String)]): FutureAction[Unit] = {
    rddTransformer(events).filter(_ != null).foreachPartitionAsync {
      iter =>
        val producer = KafkaConnector.newProducer(outBrokers, producerParams)
        iter.foreach {
          line =>
            val message = new ProducerRecord[String, String](outTopic, line)
            producer.send(message)
        }
        producer.close()
    }
  }

  def run(): Unit = {
    // 这里我们采用异步实时处理通用类引擎来处理kafka->kafka实时任务
    new KafkaAsyncProcessingStreaming(spark, kafkaSource, dataAsyncProcess).run()
  }
}
