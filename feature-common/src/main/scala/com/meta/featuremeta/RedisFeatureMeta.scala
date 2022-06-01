package com.meta.featuremeta

import com.alibaba.fastjson.JSONObject
import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import com.meta.entity.SerializeTypeEnum.SerializeTypeEnum
import com.meta.entity.SerilizeUtils
import scala.collection.JavaConversions._
import scala.collection.immutable.Map

/**
 * 特征类基础类，任何类型特征都可以基于此类进行初始化，当然也可以直接使用继承该类的初始化的子类
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
  isCompress, serializeType, defaultVal, featureType) with Serializable {

  // 默认字符编码
  private final val CHART_SET_NAME = "UTF-8"

  // 每个特征元数据类信息都不一样，需要进行复写，并且去掉前面的包等信息，只留类似 RedisFeatureMeta、RedisFloatMeta 等类名，用于后续特征解析
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
    json.put("defaultVal", defaultVal.toString)
    json.put("generic_type", generic_type)
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
    getFieldValueAny(id, fieldIds: _*)
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
