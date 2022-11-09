package com.meta.entity

import com.meta.entity

/**
 * *redis类型枚举类
 *
 * “SSD_REDIS”、“CACHE_REDIS”分别代表 SSD磁盘类型redis和cache型redis,
 * 两者协议完全一致，但是两者成本、读写性能上会有所差异，所以会被用到不同场景
 *
 * @author weitaoliang
 * @version V1.0
 * */

object RedisEnum extends Enumeration {
  type RedisEnum = Value
  val SSD_REDIS: entity.RedisEnum.Value = Value("ssd_redis")
  val CACHE_REDIS: entity.RedisEnum.Value = Value("cache_redis")
}
