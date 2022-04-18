package com.meta.spark.kafka

import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.utils.StreamingUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * kafka 集群信息
 * jedis 为记录kafka 消费offset ,如果传入是null则由kafka自己记录
 * 同一个kafka集群的多个topic用 ","隔开，不同kafka 集群的topic 用";"隔开,
 * brokers 如果多个brokers集群用";"隔开,
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class KafkaSourceStreaming(val brokers: String,
                           val topics: String,
                           val groupid: String,
                           val batchDuration: Duration,
                           val consumerParamers: Map[String, String] = Map.empty[String, String],
                           val jedisName: Option[JedisClusterName] = None
                          ) extends Serializable {

  /**
   * 通过kafka信息 获取DStreams
   */

  def getKafkaDStream(ssc: StreamingContext): DStream[(String, String)] = {
    JedisConnector(jedisName.get)
    val jedisCluster = if (jedisName.isEmpty) None else Some(JedisConnector(jedisName.get))
    val dsTreams = (brokers.split(";") zip topics.split(";")).map {
      case (broker, topic) =>
        StreamingUtils.getKafkaStream(topic, broker, groupid, jedisName, ssc, consumerParamers)
    }
    ssc.union(dsTreams)
  }
}
