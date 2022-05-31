package com.meta.conn.tdbank

import com.tencent.tdw.spark.toolkit.tdbank.TDBankProvider
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * StreamingContext with TDBank receivers
 *
 * @author weitaoliang
 */


private[meta] class TDbankStreamingContext(ssc: StreamingContext) {
  private def sparkConf = ssc.sparkContext.getConf

  def tdBankTextStream(conf: TDBanReceiverConfig, numReceivers: Int): DStream[String] = {
    val tdbank = new TDBankProvider(ssc)

    val sparkDynamicAllocationEnabled =
      sparkConf.getBoolean("spark.dynamicAllocation.enabled", false)
    val sparkStreamingDynamicAllocationEnabled =
      sparkConf.getBoolean("spark.streaming.dynamicAllocation.enabled", false)

    // config numberReceiver when  numReceivers =0 set  numReceivers = numExecutors
    // need spark shutdown  dynamicAllocation
    val numExecutors = sparkConf.getInt("spark.executor.instances", 0)

    if (numReceivers == 0) {
      if (sparkDynamicAllocationEnabled
        || sparkStreamingDynamicAllocationEnabled
        || numExecutors == 0) {
        throw new IllegalArgumentException(
          s"Dynamic allocation enabled, numReceivers incorrect need to > 0, $numReceivers > 0 ?")
      }
    }
    tdbank.textStream(conf, numExecutors)
  }
}
// 伴生对象，使用隐式函数，提供优雅便捷的方法调用
private[meta] object TDbankStreamingContext {
  implicit def fromStreamingContext(ssc: StreamingContext): TDbankStreamingContext = {
    new TDbankStreamingContext(ssc)
  }
}