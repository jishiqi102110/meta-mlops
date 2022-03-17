package com.meta

import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.{FeatureDTO, FeatureTypeEnum, RedisEnum}
import com.meta.featuremeta.RedisIntMeta
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.collection.mutable

/**
 * @author weitaoliang
 * @version V1.0
 * */


class UnitTest {
  // scalastyle:off println
  @Test
  def testFeatureDTO(): Unit = {

    val floatValue = FeatureDTO.FieldValue.newBuilder
      .setValueType(FeatureDTO.FieldValue.ValueType.FLOAT)
      .setValue(FeatureDTO.Value.newBuilder.setFloatVal(0.1f))
      .build

    val bytes = floatValue.toByteArray

    val floatValueParse = FeatureDTO.FieldValue.parseFrom(bytes)
    println(floatValue)
  }

  @Test
  def testInt32: Unit = {

    val intValue = FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(1))
      .build()
    val intValue2 = FeatureDTO.FieldValue.newBuilder()
      .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(2))
      .build()
    val intMeta = new RedisIntMeta(JedisClusterName.test_cache1,
      "user_commonKey:{device_id}",
      "age",
      "testTDW",
      intValue,
      FeatureTypeEnum.USER)
    intMeta.register()
    intMeta.save("device_id2", intValue2)
    println("value:" + intMeta.get("device_id2"))
    // field 占位符测试
    val intFieldMeta = new RedisIntMeta(JedisClusterName.test_cache1,
      "user_commonKey:{device_id}",
      "cross_{create_id}",
      "user_common_tdw",
      intValue,
      FeatureTypeEnum.USER)

    println("keyplaceHolder:" + intFieldMeta.keyPlaceHolder)

    intFieldMeta.register()
    val fieldMap = mutable.HashMap.empty[String, FieldValue]
    fieldMap.put("id1", intValue)
    fieldMap.put("id2", intValue2)
    intFieldMeta.saveField("device_id1", fieldMap.toMap)
    val arr = List("id1", "id2")
    System.out.println(intFieldMeta.getFieldValue("device_id1", arr: _*))

    intMeta.expire("device_id1", 3600)
    System.out.println(intMeta.ttl("device_id1"))

    System.out.println(intMeta.exists("device_id1"))

    System.out.println(intMeta.exists("device_id2"))

    System.out.println(intMeta.serializeAny(intValue))


  }
  // scalastyle:on println

}
