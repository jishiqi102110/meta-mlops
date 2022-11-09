package com.meta.sys

import com.meta.utils.MLUtils
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


  // method longer than 50 lines
  // scalastyle:off
  @Test
  def testRunSql(): Unit = {
    // 这里必须加个bin 目录，不加会报找不到utils,这里设置后就不用去设置hadoop环境变量了
    val hadoopDir = "D:\\bin\\winutils.exe"
    val spark = MLUtils.getLocalSparkSession("testLocal", hadoopDir)
    import spark.implicits._

    val arr = Array(("id1", 2, "f1"), ("id2", 1, "f2"), ("id3", 3, "f3"))

    // 构造特征入库DF,其中必须包含特征 redisKeyPattern 的填充符,如下的例子就是“id” ,需要入库注册的特征包括 intValue、StringValue
    val df = spark.sparkContext.parallelize(arr).toDF("id", "intValue", "stringValue")
    df.createTempView("DF")

    // 定义用户sql,查询数据
    val sql = "select id,intValue,stringValue from DF"

    // 数据展示
    spark.sql(sql).show()

    // 特征默认值填充
    val defaultValue = Map(
      "intValue" -> 0,
      "stringValue" -> "null"
    )

    // 调用入库函数进行特征注册及入库
    Hive2RedisUtils.runSql(
      spark,
      sql,
      JedisClusterName.test_cache1,
      "testFeature:{id}",
      "tempView-DF",
      defaultValue,
      batch_size = 2
    )

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


}

// method longer than 50 lines
// scalastyle:on
