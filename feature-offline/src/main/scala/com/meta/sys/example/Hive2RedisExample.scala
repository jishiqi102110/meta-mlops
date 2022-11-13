//package com.meta.sys.example
//
//import com.meta.utils.MLUtils
//import com.meta.app.{AbstractAPP, Config}
//import com.meta.app.config.Hive2RedisParams
//import com.meta.conn.redis.JedisClusterName
//import com.meta.sys.Hive2RedisUtils
//import com.tencent.tdw.spark.toolkit.tdw.TDWUtil
//import org.apache.spark.sql.SparkSession
//
///**
// *
// *
// * @author weitaoliang
// * @version V1.0
// */
//object Hive2RedisExample {
//
//  def main(args: Array[String]): Unit = {
//    run(args)
//  }
//
//  def run(args: Array[String]): Unit = {
//
//    val hive2RedisParams = new Hive2RedisParams(args)
//
//    val spark = MLUtils.getSparkSession(hive2RedisParams.appName)
//    val sc = spark.sparkContext
//    import spark.implicits._ //scalastyle:ignore
//
//    val arr = Array(("id1", 2, "f1"), ("id2", 1, "f2"), ("id3", 3, "f3"))
//
//    // 构造特征入库DF,其中必须包含特征 redisKeyPattern 的填充符,如下的例子就是“id” ,需要入库注册的特征包括 intValue、StringValue
//    val df = spark.sparkContext.parallelize(arr).toDF("id", "intValue", "stringValue")
//    df.createTempView("DF")
//
//    // 定义用户sql,查询数据
//    val sql = "select id,intValue,stringValue from DF"
//
//    // 数据展示
//    spark.sql(sql).show()
//
//    // 特征默认值填充
//    val defaultValue = Map(
//      "intValue" -> 0,
//      "stringValue" -> "null"
//    )
//    // 调用入库函数进行特征注册及入库
//    Hive2RedisUtils.runSql(
//      spark,
//      sql,
//      JedisClusterName.test_cache1,
//      "testFeature:{id}",
//      "tempView-DF",
//      defaultValue,
//      batch_size = 2
//    )
//  }
//}
