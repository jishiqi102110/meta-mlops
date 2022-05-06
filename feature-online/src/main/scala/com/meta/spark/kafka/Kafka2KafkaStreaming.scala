package com.meta.spark.kafka

import com.meta.spark.kafka.conn.KafkaConnector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.FutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 将数据从一个kafka发送到另外一个kafka
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
  private def dataAsynProcess(events: RDD[(String, String)]): FutureAction[Unit] = {
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
    new KafkaAsynProcessingStreaming(spark, kafkaSource, dataAsynProcess).run()
  }
}
