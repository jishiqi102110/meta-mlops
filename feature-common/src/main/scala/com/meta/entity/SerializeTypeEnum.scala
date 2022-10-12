package com.meta.entity

/**
 * 特征序列化枚举类
 *
 * @author weitaoliang
 * @version V1.0
 * */

object SerializeTypeEnum extends Enumeration {
  type SerializeTypeEnum = Value
  val BYTES: Value = Value("bytes")
  val PROTO: Value = Value("proto")
}
