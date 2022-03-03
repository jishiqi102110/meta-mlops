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

    //field 占位符测试

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


  @Test
  def testRunSql(): Unit = {
    // 这里必须加个bin 目录，不加会报找不到utils,这里设置后就不用去设置hadoop环境变量了
    System.setProperty("HADOOP_HOME", "D:\\bin\\winutils.exe")

    val spark = SparkSession
      .builder()
      .appName("localTest")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val arr = Array(("id1", 2, "f1"), ("id2", 1, "f2"), ("id3", 3, "f3"))
    val df = spark.sparkContext.parallelize(arr).toDF("id", "intValue", "stringValue")
    df.createTempView("DF")

    val sql = "select id,intValue,stringValue from DF limit 2"
    spark.sql(sql).show()
    val defaultValue = Map(
      "intValue" -> 0,
      "stringValue" -> "null"
    )

    //    Hive2RedisUtils.runSql(
    //      spark,
    //      sql,
    //      jedisClusterName,
    //      "testFeature:{id}",
    //      "tempView-DF",
    //      defaultValue,
    //      checkButton = true
    //    )

    //ok 入库成功
    //测试获取

    val intValue = FeatureDTO.FieldValue.newBuilder().
      setValueType(FeatureDTO.FieldValue.ValueType.INT32).
      setValue(FeatureDTO.Value.newBuilder.setInt32Val(1)).
      build()


    val intFieldMeta = new RedisIntMeta(JedisClusterName.test_cache1,
      "testFeature:{id}",
      "intValue",
      "user_common_tdw",
      intValue,
      FeatureTypeEnum.USER)
    println(intFieldMeta.get("id1"))
    println(intFieldMeta.get("id2"))
    println(intFieldMeta.get("id3"))

    intFieldMeta.register()


  }

}
