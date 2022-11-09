package com.meta.entity

/**
 * 特征序列化枚举类
 *
 * “BYTES”、“PROTO”两种序列化方式分别代表，原生bytes与proto序列化方式，被使用在特征序列化上
 *
 * @author weitaoliang
 * @version V1.0
 * */

object SerializeTypeEnum extends Enumeration {
  type SerializeTypeEnum = Value
  val BYTES: Value = Value("bytes")
  val PROTO: Value = Value("proto")
}
