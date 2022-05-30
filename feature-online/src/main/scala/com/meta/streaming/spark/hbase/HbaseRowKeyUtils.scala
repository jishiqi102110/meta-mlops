package com.meta.streaming.spark.hbase

/**
 * hbase rowkey生成工具
 *
 * @author: weitaoliang
 * @version v1.0
 * */
object HbaseRowKeyUtils {
  // hbase 默认分区数
  private final val DEFAULT_PARTITION_NUM = 1000

  // 根据token 及adid 生成rowkey
  // ttl 过期时间,hbase表设置时间一致
  // rowkey rowkey 分桶id(3byte) + 时间戳（10byte）+ uuid(9byte) +adid(10byte)  = 32 byte
  def getKey(uuid: String, timeStamp: Long, id: String, ttl: Long): String = {
    val key = new StringBuilder
    // 时间戳后三位为分桶id(3)
    key.append(getHashID(uuid, DEFAULT_PARTITION_NUM))
    // 时间戳变成ttl余数
    key.append((timeStamp % ttl / 1000).formatted("%010d"))
    // uuid取9位
    key.append(getSampledUUID(uuid, timeStamp))
    // adid取10位
    key.append(id.formatted("%10s").replaceAll(" ", "\\0"))
    key.toString()
  }

  // 根据token 生成rowkey,一个请求对应一个rowkey
  // rowkey 分桶id(3byte) +时间戳（10byte）+uuid(9byte)
  def getKey(uuid: String, timeStamp: Long, ttl: Long,
             partitionNum: Int = DEFAULT_PARTITION_NUM): String = {
    val key = new StringBuilder
    // 时间戳后3位作为分桶id
    key.append(getHashID(uuid, partitionNum))
    // 时间戳变成ttl余数
    key.append((timeStamp % ttl / 1000).formatted("%010d"))
    // uuid取9位
    key.append(getSampledUUID(uuid, timeStamp))
    key.toString()
  }

  def getHashID(key: Any, partitionNum: Int): String = {
    val code = if (key == null) 0 else key.hashCode() ^ (key.hashCode() >>> 16)
    val len = (partitionNum - 1).toString.length
    Math.abs(code % partitionNum).formatted(s"%0${len}d")
  }

  private def getSampledUUID(uuid: String, timwStamp: Long): String = {
    val str = new StringBuilder

    val start = (timwStamp % 10).toInt
    for (i <- 0 until (9)) {
      str.append(uuid.charAt(start + i * 2))
    }
    str.toString()
  }
}
