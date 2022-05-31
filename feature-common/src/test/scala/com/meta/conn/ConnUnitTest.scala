package com.meta.conn

import com.meta.conn.hbase.{HbaseConnectInfo, HbaseConnector}
import com.meta.conn.redis.JedisClusterName
import com.meta.entity.RedisEnum
import org.junit.Test

/**
 * 连接器测试类
 *
 * @author weitaoliang
 * @version V1.0
 */
class ConnUnitTest {
  // hbase使用示例
  @Test
  def testHbase(): Unit = {
    // 构建hbase连接器，只需要调用连接器伴生对象的apply函数
    val h1 = new HbaseConnectInfo("testHbase", "XXX", "XXX")

    // 创建方式1，默认创建方式，直接传入 HbaseConnectInfo
    val conn = HbaseConnector(h1)
    // 创建方式2也可以通过传入配置创建连接,这个方法可以传入更多参数
    val configMap = Map(
      ("hbase.zookeeper.property.clientPort", "XXX"),
      ("hbase.zookeeper.quorum", "XXX")
    )
    val conn2 = HbaseConnector.createConnector(configMap)
  }

  // redis 使用示例
  @Test
  def testRedis(): Unit = {
    val test_cache = new JedisClusterName(
      "cache1",
      "9.134.105.30",
      6380, "redis@yky123",
      30000, RedisEnum.CACHE_REDIS)
  }

}
