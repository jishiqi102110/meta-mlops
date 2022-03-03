package com.meta.featuremeta

import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import com.meta.entity.SerializeTypeEnum.SerializeTypeEnum
import scala.util.matching.Regex

/**
 * @author weitaoliang
 * @version V1.0
 * */

abstract class RedisFeatureInfo(val jedisClusterName: JedisClusterName,
                                val redisKeyPattern: String,
                                val redisField: String,
                                val dataSource: String,
                                val isCompress: Boolean,
                                val serializeType: SerializeTypeEnum,
                                val defaultVal: FieldValue,
                                val featureType: FeatureTypeEnum
                               ) extends Serializable {

  protected val generic_type: String

  private val regex = "\\{(.*)}".r

  // key的占位符 类似于 key:{imei} 其中imei 就是占位符，需要动态填充
  val keyPlaceHolder: Option[String] = findFirstMatchIn(regex, redisKeyPattern)

  // filed 的占位符 和key一个道理，{}里面就是占位符，需要动态填充
  val fieldPlaceHolder: Option[String] = if (redisField == null) {
    None
  } else {
    findFirstMatchIn(regex, redisField)
  }

  protected val desc: String

  def getAny(id: String): Any

  def getFieldValueAny(id: String, fieldIds: String*): Map[String, Any]

  def saveAny(id: String, value: Any): Unit

  def saveFieldAny(id: String, fieldsValues: Map[String, Any]): Unit

  def serializeAny(obj: Any): Array[Byte]

  def deSerializeAny(bytes: Array[Byte]): Any

  def maxAndMin(t: Any): (Double, Double) = (0.0, 0.0)

  def delete(id: String): Unit = {
    if (redisField == null || fieldPlaceHolder == null) {
      JedisConnector(jedisClusterName).del(getKey(id))
    } else {
      JedisConnector(jedisClusterName).hdel(getKey(id), redisField)
    }
  }

  def expire(id: String, ttl: Int): Unit = {
    JedisConnector(jedisClusterName).expire(getKey(id), ttl)
  }

  def ttl(id: String): Long = {
    JedisConnector(jedisClusterName).ttl(getKey(id))
  }

  def exists(id: String): Boolean = {
    if (redisField == null) {
      JedisConnector(jedisClusterName).exists(getKey(id))
    } else {
      JedisConnector(jedisClusterName).hexists(getKey(id), redisField)
    }
  }

  def getKey(id: String): String = {
    if (keyPlaceHolder.isEmpty) redisKeyPattern else redisKeyPattern.replace(keyPlaceHolder.get, id)
  }

  def getField(id: String): String = {
    if (fieldPlaceHolder.isEmpty) redisField else redisField.replace(fieldPlaceHolder.get, id)
  }

  def deleteMeta(): Unit = {
    // 这里构造mysql类entity 进行meta 删除
    RedisFeatureInfo.deleteMeta()
  }

  def register(): Unit = {
    // todo 这里去注册redis、注册特征元数据
    // scalastyle:off
    println("注册特征" + desc)
  }

  override def toString: String = desc

  private def findFirstMatchIn(regex: Regex, str: String): Option[String] = {
    regex.findFirstMatchIn(str)
    match {
      case Some(m) => Some(m.group(1))
      case None => None
    }
  }

}

object RedisFeatureInfo {
  //private lazy val simpleHttp =

  private def deleteMeta(): Unit = {

  }

  private def register(): Unit = {

  }
}

