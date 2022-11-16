package com.meta.spark.kafka

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

/**
 * 单条序列特征处理类,用户使用只需要传入相关参数调用run()方法即可
 *
 * idFunctionArray Array(field,通过id获取关联id的方法，多用于交叉特征，保存最大长度，保存最大时长) 建议保存时长不超过2天
 * parseStream 解析每行topic 事件(topic,line) 转化成 （唯一标识，Array(事件事件，事件标识))
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class SequenceEventStreaming(spark: SparkSession,
                             kafkaSource: KafkaSourceStreaming,
                             redisKeyPattern: String,
                             jedisClusterName: JedisClusterName,
                             idFunctionArray: Array[(String, String => String, Int, Int)],
                             parseStream:
                             DStream[(String, String)] => DStream[(String, Array[(Long, String)])]
                            ) extends Serializable {
  private var _featureType: FeatureTypeEnum = _

  def run(): Unit = {
    val multiSequenceEventStreaming =
      new MultiSequenceEventStreaming(spark, kafkaSource, jedisClusterName)
    val events = parseStream(multiSequenceEventStreaming.kafkaStream).filter(_ != null)
    assert(_featureType == null, "必须设置featureType!!!")
    multiSequenceEventStreaming.putEvents(redisKeyPattern, _featureType, events, idFunctionArray)
  }

}
