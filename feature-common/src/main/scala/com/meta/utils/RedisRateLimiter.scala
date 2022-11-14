package com.meta.utils

import com.google.common.util.concurrent.RateLimiter
import com.meta.Logging

/**
 * 实现单进程限速器，防止spark executor cores 参数设置过多导致的redis写入过快，保障入库redis限速能力
 * 用户使用时只需要传入总体限速permit 例如5000 qps,以及任务executors 例如20，5000/20 即可进行算出每个分布式节点的单个限速要求
 *
 * @author weitaoliang
 * @version V1.0
 * @date 2022/1/20 15:55
 * */
object RedisRateLimiter extends Logging {

  @transient private var rateLimiter: RateLimiter = _
  private final val LOCK = new Object()

  /**
   * 创建限速器
   *
   * @Param [permit,executorNums]
   * @return RateLimiter
   * @Description
   * */
  def create(permit: Int, executorNums: Int): RateLimiter = {
    if (rateLimiter == null) LOCK.synchronized {
      val corePermit = permit / executorNums
      this.rateLimiter = RateLimiter.create(corePermit)
      logInfo(s"create redis rateLimiter permits $corePermit ,totalPermit $permit")
    }
    rateLimiter
  }

  /**
   * @Param
   * @return
   * @Description 获取限速器权限
   * */
  def acquire(): Double = {
    rateLimiter.acquire()
  }
}

