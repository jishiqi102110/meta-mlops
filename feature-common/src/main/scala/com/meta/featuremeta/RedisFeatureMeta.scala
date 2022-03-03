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
  isCompress, serializeType, defaultVal, featureType)
  with Serializable {

  private final val CHART_SET_NAME = "UTF-8"
  override protected val generic_type: String = {
    val classString = this.getClass.toString
    classString.substring(classString.lastIndexOf(".") + 1)
  }
  override protected val desc: String = {
    val json = new JSONObject()
    json.put("jedis", jedisClusterName.toString)
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

  override def getAny(id: String): Any = get(id)

  override def getFieldValueAny(id: String, fieldIds: String*): Map[String, Any] = {
    getFieldValueAny(id, fieldIds: _*)
  }


  override def saveAny(id: String, value: Any): Unit = save(id, value.asInstanceOf[FieldValue])

  override def saveFieldAny(id: String, fieldsValues: Map[String, Any]): Unit = {
    saveField(id, fieldsValues.mapValues(_.asInstanceOf[FieldValue]))
  }

  override def serializeAny(obj: Any): Array[Byte] = serialize(obj.asInstanceOf[FieldValue])

  override def deSerializeAny(bytes: Array[Byte]): Any = deserialize(bytes)

  def deserialize(bytes: Array[Byte], default: FieldValue = defaultVal): FieldValue = {
    SerilizeUtils.deserialize(bytes, default, isCompress)
  }

  // 反序列化方法
  def serialize(ojb: FieldValue): Array[Byte] = SerilizeUtils.serialize(ojb, isCompress)

  // 获取特征
  def get(id: String): FieldValue = {

    val bytes = if (redisField == null) {
      JedisConnector(jedisClusterName).get(getKey(id).getBytes(CHART_SET_NAME))
    } else {
      JedisConnector(jedisClusterName).hget(
        getKey(id).getBytes(CHART_SET_NAME), redisField.getBytes(CHART_SET_NAME))
    }
    deserialize(bytes)
  }

  // 存储特征
  def save(id: String, value: FieldValue): Unit = {
    if (redisField == null) {
      JedisConnector(jedisClusterName).set(
        getKey(id).getBytes(CHART_SET_NAME), serialize(value))
    } else {
      JedisConnector(jedisClusterName).hset(
        getKey(id).getBytes(CHART_SET_NAME), redisField.getBytes("UTF-8"), serialize(value))
    }
  }

  // 获取特征，field 中有占位符的
  def getFieldValue(id: String, fields: String*): Map[String, FieldValue] = {
    val lists = JedisConnector(jedisClusterName).hmget(
      getKey(id).getBytes(CHART_SET_NAME),
      fields.map(id => getField(id).getBytes(CHART_SET_NAME)): _*).
      map(deserialize(_, defaultVal))

    (fields zip lists).toMap
  }

  def saveField(id: String, fieldIdValues: Map[String, FieldValue]): Unit = {
    val map = fieldIdValues.map {
      case (fieldId, value) =>
        (getField(fieldId).getBytes(CHART_SET_NAME), serialize(value))
    }
    JedisConnector(jedisClusterName).hmset(getKey(id).getBytes(CHART_SET_NAME), map)
  }

}
