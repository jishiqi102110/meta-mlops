package com.meta.utils

import org.apache.spark.sql.SparkSession

/**
 * 机器学习工具类
 *
 * @author weitaoliang
 * @version V1.0 
 */
object MLUtils {

  def getSparkSession(appName: String): SparkSession = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()
    spark
  }

  def getLocalSparkSession(appName: String, hadoopDir: String): SparkSession = {
    System.setProperty("HADOOP_HOME", hadoopDir)
    val spark = SparkSession
      .builder()
      .appName("localTest")
      .master("local")
      .getOrCreate()
    spark
  }
}
