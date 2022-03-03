package com.meta.entity

import com.meta.entity

/**
 * *redis类型枚举类
 *
 * @author weitaoliang
 * @version V1.0
 * */

object RedisEnum extends Enumeration {
  type RedisEnum = Value
  val SSD_REDIS: entity.RedisEnum.Value = Value("ssd_redis")
  val REDIS: entity.RedisEnum.Value = Value("redis")
}
