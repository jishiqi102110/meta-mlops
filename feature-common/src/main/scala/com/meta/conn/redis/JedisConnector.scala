package com.meta.conn.redis

import com.meta.Logging
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable

object JedisConnector extends Serializable with Logging {
  @transient private lazy val redisMap = new mutable.HashMap[String, JedisPool]()

  // redis 默认链接参数，根据业务数据库设置，不能轻易修改redis配置，所以涉及线上操作必须走这个方法
  private final val POOL_MAX = 250
  private final val MAX_IDLE = 32
  private final val MIN_EVICTABLE_IDLE_TIME_MILLIS = 60000
  private final val TIME_BETWEEN_EVICTION_RUNS_MILLIS = 30000
  private final val NUM_TESTS_PER_EVICTION_RUN = -1
  private final val SLEEP_TIME_LIMIT = 500
  private final val SLEET_TIME = 4
  private final val LOCK = new Object()

  /**
   * redis连接器，返回连接池
   *
   * @Param [host, port, password, timeout]
   * @return JedisPool
   */
  private def getConnector(host: String, port: Int, password: String, timeout: Int): JedisPool = {
    val poolConfig = new JedisPoolConfig

    poolConfig.setMaxTotal(POOL_MAX)
    poolConfig.setMaxIdle(MAX_IDLE)
    poolConfig.setTestOnBorrow(false)
    poolConfig.setTestOnReturn(false)
    poolConfig.setTestWhileIdle(false)
    poolConfig.setMinEvictableIdleTimeMillis(MIN_EVICTABLE_IDLE_TIME_MILLIS)
    poolConfig.setTimeBetweenEvictionRunsMillis(TIME_BETWEEN_EVICTION_RUNS_MILLIS)
    poolConfig.setNumTestsPerEvictionRun(NUM_TESTS_PER_EVICTION_RUN)
    val jedisPool = new JedisPool(poolConfig, host, port, timeout, password)
    jedisPool
  }

  /**
   * 默认获取redis连接类
   *
   * @Param [jedisClusterName]
   * @return Jedis
   */
  def apply(jedisClusterName: JedisClusterName): Jedis = {

    if (!redisMap.contains(jedisClusterName.host)) {
      LOCK.synchronized {
        redisMap += jedisClusterName.host -> getConnector(
          jedisClusterName.host,
          jedisClusterName.port,
          jedisClusterName.auth,
          jedisClusterName.timeout)

        var sleepTime: Int = SLEET_TIME

        var jedis: Option[Jedis] = None

        while (jedis.isEmpty) {
          try {
            jedis = Some(redisMap(jedisClusterName.host).getResource)
          }
          catch {
            case e: JedisConnectionException
              if e.getCause.toString.contains("ERR max number of clients reached") =>
              if (sleepTime < SLEEP_TIME_LIMIT) sleepTime *= 2
              logWarning("*************************************")
              logWarning(s"redis ${jedisClusterName.host}库目前没有空闲的连接，$sleepTime 毫秒后会重新尝试，" +
                s"如果长时间得不到连接，请修改最大连接数的配置")
              logWarning("*************************************")
              Thread.sleep(sleepTime)
            case e: Exception => throw e
          }
        }

        logInfo("********************************************")
        logInfo(s"######初始化$jedisClusterName.host redis连接")
        logInfo("********************************************")
      }
    }
    redisMap(jedisClusterName.host).getResource
  }
}
