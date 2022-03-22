package com.meta.sys

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import com.meta.featuremeta.RedisIntMeta
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * 单元测试类
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class UnitTest {
  // scalastyle:off println

  @Test
  def testPrintln(): Unit = {
    println("hello")
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

    val sql = "select id,intValue,stringValue from DF limit 3"
    spark.sql(sql).show()
    val defaultValue = Map(
      "intValue" -> 0,
      "stringValue" -> "null"
    )

    Hive2RedisUtils.runSql(
      spark,
      sql,
      JedisClusterName.test_cache1,
      "testFeature:{id}",
      "tempView-DF",
      defaultValue,
      batch_size = 2
    )
    // ok 入库成功
    // 测试获取

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

  }

  @Test
  def testPartitionSlice(): Unit = {

    // 这里重新测试了下没有问题

    val dataList = Array(1, 2, 3, 4, 5, 6, 7)
    val batch_size = 2
    val nStep = math.ceil(dataList.size / batch_size.toDouble).toInt

    for (index <- 0 to nStep) {
      val lowerIndex = batch_size * index
      val upperIndex = if (lowerIndex + batch_size >= dataList.size) {
        dataList.size
      }
      else {
        batch_size * (index + 1)
      }
      val batchData = dataList.slice(lowerIndex, upperIndex)
      println(batchData.mkString("-"))
    }
    println(nStep)
  }
  // scalastyle:on println
}
