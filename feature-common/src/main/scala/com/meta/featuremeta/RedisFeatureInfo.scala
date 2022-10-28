package com.meta.featuremeta

import com.meta.Logging
import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import com.meta.entity.SerializeTypeEnum.SerializeTypeEnum
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/**
 * 特征抽象类
 *
 * jedisClusterName 存储的集群类
 * redisKeyPattern 特征存储key，一般形式为 常量+{deviceid},{}中为填充符
 * redisField 特征存储field,如果feild是空，则特征为kv结构，如果不为空则为hash结构，另外如果field中也包含{},里面也包含填充符,则该特征为交叉特征
 * dataSource 特征数据源，一般填存储离线(twd、hive)、实时(tdbank、kafka)地址
 * isCompress 特征是否压缩，是则采用snappy进行压缩，节约存储空间
 * serializeType 特征序列化类型，有两种一种是pb序列化，一种是原始bytes,pb更加节约内存，基本类型建议bytes,长字符串、序列特征、向量特征使用pb
 * defaultVal 特征默认值，用于注册时定义，可以减少默认值入库
 * featureType 特征类型，暂时分为 user、item、cross、scene 用户、物品、交叉、场景特征
 *
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
                                val featureType: FeatureTypeEnum) extends Serializable with Logging {

  // 用于描述特征元数据类信息
  protected val generic_type: String

  private val regex = "\\{(.*)}".r

  // key的占位符 类似于 key:{deviceid} 其中imei 就是占位符，需要动态填充
  val keyPlaceHolder: Option[String] = findFirstMatchIn(regex, redisKeyPattern)

  // filed 的占位符 和key一个道理，{}里面就是占位符，需要动态填充
  val fieldPlaceHolder: Option[String] = if (redisField == null) {
    None
  } else {
    findFirstMatchIn(regex, redisField)
  }

  // 特征描述信息
  protected val desc: String

  def getAny(id: String): Any

  def getFieldValueAny(id: String, fieldIds: String*): Map[String, Any]

  def saveAny(id: String, value: Any): Unit

  def saveFieldAny(id: String, fieldsValues: Map[String, Any]): Unit

  def serializeAny(obj: Any): Array[Byte]

  def deSerializeAny(bytes: Array[Byte]): Any

  def maxAndMin(t: Any): (Double, Double) = (0.0, 0.0)

  /**
   * 特征删除方法，只需要传入id进行特征删除，用于批量删除特征或者删除测试特征
   *
   * @Param [id] 例如deviceid
   */
  def delete(id: String): Unit = {
    if (redisField == null || fieldPlaceHolder == null) {
      JedisConnector(jedisClusterName).del(getKey(id))
    } else {
      JedisConnector(jedisClusterName).hdel(getKey(id), redisField)
    }
  }

  /**
   * 设置特征过期方法
   *
   * @Param [id, ttl] 只需传入id，以及过期时间即可设置特征过期时间
   */
  def expire(id: String, ttl: Int): Unit = {
    JedisConnector(jedisClusterName).expire(getKey(id), ttl)
  }

  /**
   * 查看特征过期时间
   *
   * @Param [id] 传入id查询特征过期时间
   */
  def ttl(id: String): Long = {
    JedisConnector(jedisClusterName).ttl(getKey(id))
  }

  /**
   * 判断特征是否在redis
   *
   * @Param [id] 传入id查询特征是否存在
   */
  def exists(id: String): Boolean = {
    if (redisField == null) {
      JedisConnector(jedisClusterName).exists(getKey(id))
    } else {
      JedisConnector(jedisClusterName).hexists(getKey(id), redisField)
    }
  }

  /**
   * 返回特征key
   *
   * @Param [id] 传入id,替换填充符，返回实际存储在redis中的key
   */
  def getKey(id: String): String = {
    if (keyPlaceHolder.isEmpty) redisKeyPattern else redisKeyPattern.replace(keyPlaceHolder.get, id)
  }

  /**
   * 返回特征field
   *
   * @Param [id] 传入id,替换填充符，返回实际存储在redis中的field
   */
  def getField(id: String): String = {
    if (fieldPlaceHolder.isEmpty) redisField else redisField.replace(fieldPlaceHolder.get, id)
  }

  /** 删除特征meta元信息接口 */
  def deleteMeta(): Unit = {
    RedisFeatureInfo.deleteMeta()
  }

  /** 注册特征接口，注册元数据信息到特征平台 */
  def register(): Unit = {
    // todo 这里去注册redis集群、特征元数据到特征平台
    logInfo("**********************************************")
    logInfo(s"register feature $desc")
    logInfo("**********************************************")
  }

  /** 特征toString方法 */
  override def toString: String = desc

  /**
   * 此方法用于匹配正则表达式，用来替换特征key和field中的填充符
   *
   * @Param [regex] 正则表达式
   * @Param [str] 需要替换的字符串
   */
  private def findFirstMatchIn(regex: Regex, str: String): Option[String] = {
    regex.findFirstMatchIn(str)
    match {
      case Some(m) => Some(m.group(1))
      case None => None
    }
  }
}

/** 伴生类 */
private[meta] object RedisFeatureInfo {

  def apply(jedisClusterName: JedisClusterName,
            redisKeyPattern: String,
            redisField: String,
            dataSource: String,
            isCompress: Boolean,
            serializeType: SerializeTypeEnum,
            defaultVal: FieldValue,
            featureType: FeatureTypeEnum): RedisFeatureInfo = ???

  // private lazy val simpleHttp =

  /** 特征删除方法实现 */
  private def deleteMeta(): Unit = {

  }

  /** 特征注册方法实现 */
  private def register(): Unit = {

  }
}

