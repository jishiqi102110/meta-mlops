package com.meta.conn.redis

import com.meta.entity.RedisEnum
import com.meta.entity.RedisEnum.RedisEnum

/**
 * redis 连接器抽象类,用于描述redis
 *
 * @author weitaoliang
 * @version V1.0
 * */
class JedisClusterName(val name: String, // redis名称，自定义
                       val host: String,// redis host地址
                       val port: Int, // redis 端口
                       val auth: String,// redis 密码
                       val timeout: Int, // redis 超时时间
                       val redisType: RedisEnum // redis 类型（ssd_redis、cache_redis）
                      ) extends Serializable {
  override def toString: String = {
    "name:" + name + "->host:" + host + ":" + port
  }
}

object JedisClusterName {

  // 这是一个测试的集群账号,提前可以在伴生对象中进行定义，后续直接引用即可
  // scalastyle:off
  val test_cache1 = new JedisClusterName(
    "cache1",
    "9.134.105.30",
    6380, "redis@yky123",
    30000, RedisEnum.CACHE_REDIS)

}

