package com.meta.conn.hbase

import com.meta.Logging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * hbase 连接器
 *
 * @author: weitaoliang
 * @version v1.0
 * */
object HbaseConnector extends Serializable with Logging {

  @transient private lazy val connectMap = new mutable.HashMap[String, Connection]()

  private final val LOCK = new Object()

  /**
   * apply函数,默认构建连接方式
   *
   * @Param [hbaseConnectInfo]
   * @return 返回Connection
   */
  def apply(hbaseConnectInfo: HbaseConnectInfo): Connection = {

    if (!connectMap.contains(hbaseConnectInfo.name)) {
      // 保证线程安全，防止spark多核运行时的连接过多问题
      LOCK.synchronized {
        if (!connectMap.contains(hbaseConnectInfo.name)) {
          val configMap = Map(
            ("hbase.zookeeper.property.clientPort", hbaseConnectInfo.zookeeperPort),
            ("hbase.zookeeper.quorum", hbaseConnectInfo.zookeeperQuorum)
          )
          connectMap += hbaseConnectInfo.name -> createConnector(configMap)
          logInfo("#########################################")
          logInfo(s"初始化 hbase 连接器 ${hbaseConnectInfo.name}!!!")
          logInfo("#########################################")
        }
      }
    }
    connectMap(hbaseConnectInfo.name)
  }

  /**
   * 提供更高级别连接器，丰富参数
   *
   * @Param [hbaseConfig]
   * @return 返回Connection
   */
  def createConnector(hbaseConfig: Map[String, String]): Connection = {

    val config = HBaseConfiguration.create()
    for ((k, v) <- hbaseConfig) {
      config.set(k, v)
    }
    ConnectionFactory.createConnection(config)
  }
}
