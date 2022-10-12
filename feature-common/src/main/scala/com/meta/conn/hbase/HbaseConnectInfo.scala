package com.meta.conn.hbase

/**
 * hbase 连接信息类
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class HbaseConnectInfo(val name: String, // hbase集群名称
                       val zookeeperQuorum: String, // hbase zookeeperQuorum 地址
                       val zookeeperPort: String // hbase port地址
                      ) extends Serializable
// 伴生类
object HbaseConnectInfo {
  // 测试集群
  val testHBase = new HbaseConnectInfo("testHbase", "XXX", "XXX")
}
