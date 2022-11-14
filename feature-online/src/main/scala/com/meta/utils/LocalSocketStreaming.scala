package com.meta.utils

import com.meta.spark.kafka.KafkaSourceStreaming
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * 本地SocketStreaming发射器，可以进行本地模拟任务
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class LocalSocketStreaming(inputPath: Array[String],
                           port: Int,
                           batchDuration: Duration)
  extends KafkaSourceStreaming("", "", "",
    batchDuration, Map.empty[String, String], None) with Serializable {
  override def getKafkaDStream(ssc: StreamingContext): DStream[(String, String)] = {
    val socketRunnable = new SocketThread(inputPath, port)
    val t = new Thread(socketRunnable)
    t.start()
    Thread.sleep(1000)

    val localSocketStream = ssc.socketTextStream("localhost", port)
    localSocketStream.map {
      line =>
        ("localSocket", line)
    }
  }

}
