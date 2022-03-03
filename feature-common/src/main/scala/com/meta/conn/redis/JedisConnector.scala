package com.meta.conn.redis

import com.sun.org.slf4j.internal.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable

object JedisConnector extends Serializable {
  private val logger = LoggerFactory.getLogger(JedisConnector.getClass)
  @transient private lazy val redisMap = new mutable.HashMap[String, JedisPool]()

  private final val POOL_MAX = 250
  private final val MAX_IDLE = 32
  private final val MIN_EVICTABLE_IDLE_TIME_MILLIS = 60000
  private final val TIME_BETWEEN_EVICTION_RUNS_MILLIS = 30000
  private final val NUM_TESTS_PER_EVICTION_RUN = -1
  private final val SLEEP_TIME_LIMIT = 500
  private final val SLEET_TIME = 4

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


  def apply(jedisClusterName: JedisClusterName): Jedis = {

    if (!redisMap.contains(jedisClusterName.host)) {
      JedisConnector.synchronized {
        redisMap += jedisClusterName.host -> getConnector(
          jedisClusterName.host,
          jedisClusterName.port,
          jedisClusterName.auth,
          jedisClusterName.timeout)

        var sleepTime: Int = SLEET_TIME
        // scalastyle:off
        var jedis: Jedis = null

        while (jedis == null) {
          try {
            jedis = redisMap(jedisClusterName.host).getResource
          }
          catch {
            case e: JedisConnectionException
              if e.getCause.toString.contains("ERR max number of clients reached") =>
              if (sleepTime < SLEEP_TIME_LIMIT) sleepTime *= 2
              logger.warn("*************************************")
              logger.warn(s"redis ${jedisClusterName.host}库目前没有空闲的连接，$sleepTime 毫秒后会重新尝试，" +
              s"如果长时间得不到连接，请修改最大连接数的配置")
              logger.warn("*************************************")
              Thread.sleep(sleepTime)
            case e: Exception => throw e

          }
        }

        logger.warn("********************************************")
        logger.warn(s"######初始化$jedisClusterName.host redis连接")
        logger.warn("********************************************")
      }
    }
    redisMap(jedisClusterName.host).getResource
  }
}
