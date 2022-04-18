package com.meta.utils

import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * description 
 *
 * @author: weitaoliang
 * @version v1.0
 * */
object StreamingUtils {

  def getKafkaStream(topic: String,
                     broker: String,
                     groupid: String,
                     jedis: Option[JedisClusterName],
                     ssc: StreamingContext,
                     consumerParams: Map[String, String]
                    ): DStream[(String,String)] = {
    null
  }

}
