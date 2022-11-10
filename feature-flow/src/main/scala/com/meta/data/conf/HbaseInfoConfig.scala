package com.meta.data.conf

import com.meta.conn.hbase.{HbaseConnectInfo, HbaseConnector}
import com.meta.spark.hbase.HbaseRowKeyUtils
import com.meta.data.utils.FlowConstant

import org.apache.hadoop.hbase.client.{Put}
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
                      var partitionNum: Int = FlowConstant.HBASE_PRE_PARTITION_NUM)
  extends Serializable {
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
