package com.meta.spark.kafka

import com.meta.Logging
import com.meta.conn.hbase.{HbaseConnectInfo, HbaseConnector}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Durability, Put}
import org.apache.spark.FutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._ // scalastyle:ignore

/**
 * kafka数据入库hbase 通用类，用户只需要使用此类，即可完成kafka数据入库任务
 *
 * spark SparkSession
 * kafkaSource [[KafkaSourceStreaming]] kafka相关信息配置类
 * hbaseConnectInfo [[HbaseConnectInfo]] hbase相关信息配置类
 * hbaseTableName 需要入库的hbaseTable
 * eventRDD2PutRDD: RDD[(String, String)] => RDD[Put] 用户自定义RDD[(String, String)]转化为 RDD[Put] 的方法，及用户自定义代码区
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class Kafka2HabseStreaming(spark: SparkSession,
                           kafkaSource: KafkaSourceStreaming,
                           hbaseConnectInfo: HbaseConnectInfo,
                           hbaseTableName: String,
                           eventRDD2PutRDD: RDD[(String, String)] => RDD[Put]
                          ) extends Serializable with Logging {
  /** 异步数据处理方法RDD[String, String] =》FutureAction[Unit] */
  private def dataAsyncProcess(events: RDD[(String, String)]): FutureAction[Unit] = {
    // 使用用户提供的自定义事件处理方法进行获取，不同业务逻辑不同
    val hbasePutStream = eventRDD2PutRDD(events).filter(_ != null)
    hbasePutStream.map {
      put =>
        // 这里取消kafka预写日志（WAL）,通过异步及取消WAL提高写入效率,但是有丢失数据分险，权衡之后取消该功能追求高性能
        put.setDurability(Durability.SKIP_WAL)
        put
    }.foreachPartitionAsync {
      iter =>
        val conn = HbaseConnector(hbaseConnectInfo)
        val table = conn.getTable(TableName.valueOf(hbaseTableName))
        table.batch(iter.toList, null) // scalastyle:ignore
    }
  }

  /** 启动方法，继承实现该类者只需要调用这个方法就可以实现kafka数据写入Hbase */
  def run(): Unit = {
    // 这里又使用KafkaAsyncProcessingStreaming 类使用异步kafka实时处理类来初始化
    new KafkaAsyncProcessingStreaming(spark, kafkaSource, dataAsyncProcess).run()
  }

}
