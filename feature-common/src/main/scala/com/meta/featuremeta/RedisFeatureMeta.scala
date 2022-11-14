package com.meta.featuremeta

import com.alibaba.fastjson.JSONObject
import com.meta.Logging
import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureDTO.FieldValue.ValueType
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import com.meta.entity.SerializeTypeEnum.SerializeTypeEnum
import com.meta.entity.{FeatureDTO, SerializeTypeEnum, SerilizeUtils}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.Map

/**
 * 特征类基础类，任何类型特征都可以基于此类进行初始化，当然也可以直接使用继承该类的初始化的子类
 *
 *
 * 特征抽象按照PB类[[FeatureDTO]]进行存储
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


///////////////////////////////////////////////////////////////////////////
// meta case class define
///////////////////////////////////////////////////////////////////////////

/** 对应的是 [[FeatureDTO.FLOAT]],默认采用bytes序列化方式及不采用压缩 */
case class RedisFloatMeta(override val jedisClusterName: JedisClusterName,
                          override val redisKeyPattern: String,
                          override val redisField: String,
                          override val dataSource: String,
                          override val defaultVal: FieldValue,
                          override val featureType: FeatureTypeEnum)
  extends RedisFeatureMeta[FeatureDTO.FLOAT](
    jedisClusterName, redisKeyPattern, redisField, dataSource, false,
    SerializeTypeEnum.BYTES, defaultVal, featureType) with Serializable {
  override def maxAndMin(t: Any): (Double, Double) = (t.asInstanceOf[Float], t.asInstanceOf[Float])
}


/** 对应的是 [[FeatureDTO.INT32]] 默认采用bytes序列化方式及不采用压缩 */
case class RedisIntMeta(override val jedisClusterName: JedisClusterName,
                        override val redisKeyPattern: String,
                        override val redisField: String,
                        override val dataSource: String,
                        override val defaultVal: FieldValue,
                        override val featureType: FeatureTypeEnum
                       ) extends RedisFeatureMeta[FeatureDTO.INT32](
  jedisClusterName, redisKeyPattern, redisField, dataSource, false,
  SerializeTypeEnum.BYTES, defaultVal, featureType) with Serializable {

  // 计算最大值最小值方法用于特征值监控
  override def maxAndMin(t: Any): (Double, Double) = (t.asInstanceOf[Int], t.asInstanceOf[Int])
}

/** 对应的是 [[FeatureDTO.STRING]],默认采用proto序列化方式算法工程师自行决定是否snappy压缩 */
case class RedisStringMeta(override val jedisClusterName: JedisClusterName,
                           override val redisKeyPattern: String,
                           override val redisField: String,
                           override val dataSource: String,
                           override val isCompress: Boolean,
                           override val defaultVal: FieldValue,
                           override val featureType: FeatureTypeEnum)
  extends RedisFeatureMeta[FeatureDTO.STRING](
    jedisClusterName, redisKeyPattern, redisField, dataSource, isCompress, SerializeTypeEnum.PROTO,
    defaultVal, featureType) with Serializable

/** 对应的是 [[FeatureDTO.FloatList]],默认采用proto序列化方式及snappy压缩 */
case class RedisFloatListMeta(override val jedisClusterName: JedisClusterName,
                              override val redisKeyPattern: String,
                              override val redisField: String,
                              override val dataSource: String,
                              override val defaultVal: FieldValue,
                              override val featureType: FeatureTypeEnum)
  extends RedisFeatureMeta[FeatureDTO.FloatList](
    jedisClusterName, redisKeyPattern, redisField, dataSource,
    true, SerializeTypeEnum.PROTO, defaultVal, featureType) with Serializable

/** 对应的是 [[FeatureDTO.FloatList]] 默认采用proto序列化方式及snappy压缩 */
case class RedisIntListMeta(override val jedisClusterName: JedisClusterName,
                            override val redisKeyPattern: String,
                            override val redisField: String,
                            override val dataSource: String,
                            override val defaultVal: FieldValue,
                            override val featureType: FeatureTypeEnum)
  extends RedisFeatureMeta[FeatureDTO.Int32List](
    jedisClusterName, redisKeyPattern, redisField, dataSource, true, SerializeTypeEnum.PROTO,
    defaultVal, featureType) with Serializable

/** 对应的是 [[FeatureDTO.Int64List]],默认采用proto序列化方式及snappy压缩 */
case class RedisLongListMeta(override val jedisClusterName: JedisClusterName,
                             override val redisKeyPattern: String,
                             override val redisField: String,
                             override val dataSource: String,
                             override val defaultVal: FieldValue,
                             override val featureType: FeatureTypeEnum)
  extends RedisFeatureMeta[FeatureDTO.FloatList](
    jedisClusterName, redisKeyPattern, redisField, dataSource,
    true, SerializeTypeEnum.PROTO, defaultVal, featureType) with Serializable

/** 对应的是 [[FeatureDTO.MAP_STRING_FLOAT]] 默认采用proto序列化方式及snappy压缩 */
case class RedisMapFloatMeta(override val jedisClusterName: JedisClusterName,
                             override val redisKeyPattern: String,
                             override val redisField: String,
                             override val dataSource: String,
                             override val defaultVal: FieldValue,
                             override val featureType: FeatureTypeEnum
                            )
  extends RedisFeatureMeta[FeatureDTO.MAP_STRING_FLOAT](
    jedisClusterName, redisKeyPattern, redisField, dataSource, true, SerializeTypeEnum.PROTO,
    defaultVal, featureType) with Serializable

/** 对应的是 [[FeatureDTO.MAP_STRING_FLOAT]]  默认采用proto序列化方式及snappy压缩 */
case class RedisMapStringMeta(override val jedisClusterName: JedisClusterName,
                              override val redisKeyPattern: String,
                              override val redisField: String,
                              override val dataSource: String,
                              override val defaultVal: FieldValue,
                              override val featureType: FeatureTypeEnum
                             ) extends RedisFeatureMeta[FeatureDTO.MAP_STRING_STRING](
  jedisClusterName, redisKeyPattern, redisField, dataSource, true, SerializeTypeEnum.PROTO,
  defaultVal, featureType) with Serializable

/**
 * 对应的是 [[FeatureDTO.SeqList]],默认采用proto序列化方式及snappy压缩
 * 序列特征格式，存储的格式还是为Array[String]，但是格式固定，为针对当前业务专门为序列特征抽象的格式，
 * 格式为timeStamp1:x1    timeStamp2:y1  一个时间戳对应一个序列
 */
case class RedisSeqListMeta(override val jedisClusterName: JedisClusterName,
                            override val redisKeyPattern: String,
                            override val redisField: String,
                            override val dataSource: String,
                            override val defaultVal: FieldValue,
                            override val featureType: FeatureTypeEnum)
  extends RedisFeatureMeta[FeatureDTO.SeqList](
    jedisClusterName, redisKeyPattern, redisField, dataSource, true, SerializeTypeEnum.PROTO,
    defaultVal, featureType) with Serializable

/** 对应的是 [[FeatureDTO.StringList]],默认采用proto序列化方式及snappy压缩 */
case class RedisStringListMeta(override val jedisClusterName: JedisClusterName,
                               override val redisKeyPattern: String,
                               override val redisField: String,
                               override val dataSource: String,
                               override val defaultVal: FieldValue,
                               override val featureType: FeatureTypeEnum)
  extends RedisFeatureMeta[FeatureDTO.FloatList](
    jedisClusterName, redisKeyPattern, redisField, dataSource,
    true, SerializeTypeEnum.PROTO, defaultVal, featureType) with Serializable


///////////////////////////////////////////////////////////////////////////
// meta define
///////////////////////////////////////////////////////////////////////////

/** Feature类父类，用户可以定义除了目前已经定义的 10种预定义类型特征，根据自己压缩和序列化方式进行设计 */
class RedisFeatureMeta[T](jedisClusterName: JedisClusterName,
                          redisKeyPattern: String,
                          redisField: String,
                          dataSource: String,
                          isCompress: Boolean,
                          serializeType: SerializeTypeEnum,
                          defaultVal: FieldValue,
                          featureType: FeatureTypeEnum
                         ) extends RedisFeatureInfo(
  jedisClusterName, redisKeyPattern, redisField, dataSource,
  isCompress, serializeType, defaultVal, featureType)
  with Serializable with Logging {

  // 默认字符编码
  private final val CHART_SET_NAME = "UTF-8"

  // 每个特征元数据类信息都不一样，需要进行复写，并且去掉前面的包等信息，
  // 只留类似 RedisFeatureMeta、RedisFloatMeta 等类名,用于后续特征解析
  override protected val generic_type: String = {
    val classString = this.getClass.toString
    classString.substring(classString.lastIndexOf(".") + 1)
  }

  // 重写特征描述信息
  override protected val desc: String = {
    val json = new JSONObject()
    json.put("jedis", jedisClusterName.name)
    json.put("data_source", dataSource)
    json.put("redisKeyPattern", redisKeyPattern)
    json.put("redisField", redisField)
    json.put("dataSource", dataSource)
    json.put("isCompress", isCompress)
    json.put("serializeType", serializeType.toString)
    json.put("defaultVal", RedisFeatureMeta.parseFeatureField(defaultVal))
    json.put("generic_type", generic_type)
    json.put("featureType", featureType.toString)
    json.toJSONString
  }


  /** 存储特征方法 */
  override def saveAny(id: String, value: Any): Unit = save(id, value.asInstanceOf[FieldValue])

  /** 获取特征方法 */
  override def getAny(id: String): Any = get(id)

  /** 存储非交叉特征 */
  def save(id: String, value: FieldValue): Unit = {
    if (redisField == null) {
      JedisConnector(jedisClusterName).set(
        getKey(id).getBytes(CHART_SET_NAME), serialize(value))
    } else {
      JedisConnector(jedisClusterName).hset(
        getKey(id).getBytes(CHART_SET_NAME), redisField.getBytes("UTF-8"), serialize(value))
    }
  }

  /** 获取非交叉特征 */
  def get(id: String): FieldValue = {

    val bytes = if (redisField == null) {
      JedisConnector(jedisClusterName).get(getKey(id).getBytes(CHART_SET_NAME))
    } else {
      JedisConnector(jedisClusterName).hget(
        getKey(id).getBytes(CHART_SET_NAME), redisField.getBytes(CHART_SET_NAME))
    }
    deserialize(bytes)
  }

  /**
   * 存储交叉特征，必须传入key id ,field id 以及field 对应的值，此时特征值类型为 FieldValue
   *
   * @Param [id] key id
   * @Param [fieldsValues] field id 对应的值
   */
  def saveField(id: String, fieldIdValues: Map[String, FieldValue]): Unit = {
    val map = fieldIdValues.map {
      case (fieldId, value) =>
        (getField(fieldId).getBytes(CHART_SET_NAME), serialize(value))
    }
    JedisConnector(jedisClusterName).hmset(getKey(id).getBytes(CHART_SET_NAME), map)
  }

  /**
   * 获取交叉特征，field 中有占位符的
   *
   * @Param [id] key id
   * @Param [fields] field id
   * @return Map[String, FieldValue]  field 及对应的特征值
   */
  def getFieldValue(id: String, fields: String*): Map[String, FieldValue] = {
    val lists = JedisConnector(jedisClusterName).hmget(
      getKey(id).getBytes(CHART_SET_NAME),
      fields.map(id => getField(id).getBytes(CHART_SET_NAME)): _*).
      map(deserialize(_, defaultVal))

    (fields zip lists).toMap
  }

  /**
   * 存储交叉特征，必须传入key id ,field id 以及field 对应的值，此时特征值类型为 Any
   *
   * @Param [id] key id
   * @Param [fieldsValues] field 对应的值
   */
  override def saveFieldAny(id: String, fieldsValues: Map[String, Any]): Unit = {
    saveField(id, fieldsValues.mapValues(_.asInstanceOf[FieldValue]))
  }

  /**
   * 获取hash 交叉类型特征，必须传入key id,field ids 可以批量获取一批特征
   *
   * @Param [id] key id
   * @Param [fieldIds] field ids
   * @return Map[String, Any] 返回每个field 对应的特征值，返回值为 Any类型
   */
  override def getFieldValueAny(id: String, fieldIds: String*): Map[String, Any] = {
    getFieldValue(id, fieldIds: _*)
  }

  /** 序列化方法 */
  def deserialize(bytes: Array[Byte], default: FieldValue = defaultVal): FieldValue = {
    SerilizeUtils.deserialize(bytes, default, isCompress)
  }

  /** 反序列化方法 */
  def serialize(ojb: FieldValue): Array[Byte] = SerilizeUtils.serialize(ojb, isCompress)

  override def serializeAny(obj: Any): Array[Byte] = serialize(obj.asInstanceOf[FieldValue])

  override def deSerializeAny(bytes: Array[Byte]): Any = deserialize(bytes)
}

// 伴生对象
object RedisFeatureMeta extends Logging {
  def apply[T](jedisClusterName: JedisClusterName,
               redisKeyPattern: String,
               redisField: String,
               dataSource: String,
               isCompress: Boolean,
               serializeType: SerializeTypeEnum,
               defaultVal: FieldValue,
               featureType: FeatureTypeEnum): RedisFeatureMeta[T] =
    new RedisFeatureMeta(jedisClusterName, redisKeyPattern, redisField,
      dataSource, isCompress, serializeType, defaultVal, featureType)

  // scalastyle:off
  // 这里复杂度超过了限制，但是业务要求
  /**
   * 特征值解析工具
   *
   * @Param [field]
   * @return Any
   */
  def parseFeatureField(field: FieldValue): Any = {
    val valueType = field.getValueType
    valueType match {
      case ValueType.STRING => field.getValue.getStringVal
      case ValueType.INT32 => field.getValue.getInt32Val
      case ValueType.INT64 => field.getValue.getInt64Val
      case ValueType.DOUBLE => field.getValue.getDoubleVal
      case ValueType.FLOAT => field.getValue.getDoubleVal
      case ValueType.BOOL => field.getValue.getBoolVal
      case ValueType.STRING_LIST => field.getValue.getStringListVal
      case ValueType.INT32_LIST => field.getValue.getInt32ListVal
      case ValueType.INT64_LIST => field.getValue.getInt64ListVal
      case ValueType.DOUBLE_LIST => field.getValue.getDoubleListVal
      case ValueType.FLOAT_LIST => field.getValue.getFloatListVal
      case ValueType.MAP_STRING_STRING => field.getValue.getMapStringStringVal
      case ValueType.MAP_STRING_FLOAT => field.getValue.getMapStringFloatVal
      case _ => throw new Exception("不支持此数据类型！")
    }
  }

  // scalastyle:on

}




