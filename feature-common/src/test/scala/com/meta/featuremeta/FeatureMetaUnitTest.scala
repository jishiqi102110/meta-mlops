package com.meta.featuremeta

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import org.junit.Test

import scala.collection.mutable

/**
 * @author weitaoliang
 * @version V1.0
 * */

// scalastyle:off
class FeatureMetaUnitTest {

  @Test
  def testFeatureDTO(): Unit = {

    // 测试FeatureDTO
    val floatValue = FeatureDTO.FieldValue.newBuilder
      .setValueType(FeatureDTO.FieldValue.ValueType.FLOAT)
      .setValue(FeatureDTO.Value.newBuilder.setFloatVal(0.1f))
      .build
    val bytes = floatValue.toByteArray
    val floatValueParse = FeatureDTO.FieldValue.parseFrom(bytes)
    println(floatValueParse)
  }

  // 测试int类型特征注册，非交叉类型，field不包含填充符{}
  @Test
  def testInt32Feature1: Unit = {

    // 1.特征值构建
    val intValue1 = FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(1))
      .build()

    val intValue2 = FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(2))
      .build()

    // 2.特征默认值构建
    val intDefaultValue = FeatureDTO.FieldValue.newBuilder()
      .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(0))
      .build()

    // 3.构造非交叉特征
    val intMeta = new RedisIntMeta(
      JedisClusterName.test_cache1, // 特征存储集群
      "user_commonKey:{device_id}", // 特征存储的key
      "age",  // 非交叉特征
      "testTDW", // 特征数据源
      intDefaultValue,  // 默认值填充
      FeatureTypeEnum.USER) // 特征类型

    // 4.调用注册函数 只需要注册一次
    intMeta.register()

    // 5.特征入库redis ,调用封装好的方法
    intMeta.save("device_id1", intValue1)
    intMeta.save("device_id2", intValue2)
    intMeta.expire("device_id1", 3600)
    intMeta.expire("device_id2", 3600)

    // 6.特征查询，检查是否正确入库
    // 查询存在的特征，正确输出
    println("value:" + intMeta.get("device_id1"))
    println("value:" + intMeta.get("device_id2"))
    // 查询不存在的特征，输出默认值
    println("value:" + intMeta.get("device_id3"))

  }
  // 测试int类型特征注册，交叉类型，field包含填充符{}
  @Test
  def testInt32Feature2: Unit = {

    // 1.特征值构建
    val intValue1 = FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(1))
      .build()

    val intValue2 = FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(2))
      .build()

    // 2.特征默认值构建
    val intDefaultValue = FeatureDTO.FieldValue.newBuilder()
      .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
      .setValue(FeatureDTO.Value.newBuilder.setInt32Val(0))
      .build()

    // 3.构造交叉特征
    val intFieldMeta = new RedisIntMeta(JedisClusterName.test_cache1, // 特征存储集群
      "user_commonKey:{device_id}", // 特征存储的key
      "cross_{create_id}",// 交叉特征(redisField中包含{}为交叉特征)
      "user_common_tdw", // 特征数据源
      intDefaultValue, // 特征默认值
      FeatureTypeEnum.USER) // 特征类型

    // 4.特征注册
    intFieldMeta.register()

    // 5.特征值构造
    val fieldMap = mutable.HashMap.empty[String, FieldValue]
    fieldMap.put("id1", intValue1)
    fieldMap.put("id2", intValue2)

    // 6.特征入库redis,调用封装好的方法
    intFieldMeta.saveField("device_id1", fieldMap.toMap)

    // 7.特征查询
    val arr = List("id1", "id2","id3")
    System.out.println(intFieldMeta.getFieldValue("device_id1", arr: _*))

    // 可以调用封装的方法调用设置特征过期时间，具体还有很多封装方法，参考[[RedisFeatureMeta]]
    intFieldMeta.expire("device_id1", 3600)
    System.out.println(intFieldMeta.ttl("device_id1"))

    // 可以调用封装的方法查看特征是否存在
    System.out.println(intFieldMeta.exists("device_id1"))
  }

  // 测试String类型特征,这里的特征值被上面 int交叉特征依赖
  @Test
  def testStringFeatureMeta: Unit ={

    // 1.特征值构建
    val stringValue1 =FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.STRING)
      .setValue(FeatureDTO.Value.newBuilder.setStringVal("id1"))
      .build()

    val stringValue2 =FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.STRING)
      .setValue(FeatureDTO.Value.newBuilder.setStringVal("id2"))
      .build()

    // 2.特征默认值构建
    val stringDefaultValue =FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.STRING)
      .setValue(FeatureDTO.Value.newBuilder.setStringVal("default_id"))
      .build()

    // 3.构造交叉特征

    val stringMeta = new RedisStringMeta(JedisClusterName.test_cache1, // 特征存储集群
      "item_commonKey:{groupid}", // 特征存储的key
      "creativeID",// 非交叉特征(redisField中不包含{})
      "item_common_tdw", // 特征数据源
      false,// 是否压缩，压缩可节省存储，如果是短特征建议不压缩，提高线上特征查询解析速度
      stringDefaultValue, // 特征默认值
      FeatureTypeEnum.ITEM) // 特征类型

    // 4.特征注册
    stringMeta.register()

    // 5.特征入库redis,调用封装好的方法
    stringMeta.save("group1",stringValue1)
    stringMeta.save("group2",stringValue2)

    // 6.特征查询，检查是否正确入库
    // 查询存在的特征，正确输出
    println("value:" + stringMeta.get("group1"))
    println("value:" + stringMeta.get("group1"))
    // 查询不存在的特征，输出默认值
    println("value:" + stringMeta.get("group3"))
  }

}
