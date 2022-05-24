package com.meta.spark.kafka.conn

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
 * kafka 连接器
 *
 * @author: weitaoliang
 * */

object KafkaConnector {

  def newProducer(brokers: String,
                  params: Map[String, String] = Map.empty[String, String])
  : KafkaProducer[String, String] = {
    val kafkaWriteParams = Map[String, String](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.COMPRESSION_TYPE_CONFIG -> "lz4") ++ params
    new KafkaProducer[String, String](kafkaWriteParams)
  }
}
