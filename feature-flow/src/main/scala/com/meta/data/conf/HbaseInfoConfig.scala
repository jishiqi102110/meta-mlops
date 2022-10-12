package com.meta.data.conf

import com.meta.conn.hbase.{HbaseConnectInfo, HbaseConnector}
import com.meta.streaming.spark.hbase.HbaseRowKeyUtils
import org.apache.hadoop.hbase.client.{Connection, ConnectionConfiguration, ConnectionFactory, Put}
import org.apache.hadoop
import org.apache.hadoop.hbase.TableName
import scala.collection.JavaConversions._

/**
 * description 
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class HbaseInfoConfig(val hbaseConnectInfo: HbaseConnectInfo,
                      val tableName: String,
                      val ttl: Long,
                      var partitionNum: Int = 1000) extends Serializable {
  // 获取rowKey
  def getRowKey(reqID: String, reqTimeStamp: Long): String = {
    HbaseRowKeyUtils.getKey(reqID, reqTimeStamp, ttl, partitionNum)
  }

  // 存储数据到hbase
  def saveData(puts: Iterator[Put]): Unit = {
    val conn = HbaseConnector(hbaseConnectInfo)
    val table = conn.getTable(TableName.valueOf(tableName))
    table.batch(puts.toList)
    table.close()
  }

}
