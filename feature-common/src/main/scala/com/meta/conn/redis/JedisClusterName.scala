package com.meta.conn.redis

import com.meta.entity.RedisEnum
import com.meta.entity.RedisEnum.RedisEnum

/**
 *
 * @author weitaoliang
 * @version V1.0
 * */
class JedisClusterName(val name: String,
                       val host: String,
                       val port: Int,
                       val auth: String,
                       val timeout: Int,
                       val redisType: RedisEnum) extends Serializable {
  override def toString: String = {
    "name:" + name + "->host:" + host + ":" + port
  }
}

object JedisClusterName {

  // 这是一个测试的集群账号
  // scalastyle:off
  val test_cache1 = new JedisClusterName(
    "cache1",
    "9.134.105.30",
    6380, "redis@yky123",
    30000, RedisEnum.REDIS)

}

