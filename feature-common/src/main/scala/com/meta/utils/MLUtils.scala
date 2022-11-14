package com.meta.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * 机器学习工具类
 *
 * @author weitaoliang
 * @version V1.0
 */
object MLUtils {

  /** 获取sparkSession */
  def getSparkSession(appName: String): SparkSession = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()
    spark
  }


  /** 获取本地SparkSession,用于本地调试，注意这里仅适用于windows */
  def getLocalSparkSession(appName: String, hadoopDir: String): SparkSession = {
    System.setProperty("HADOOP_HOME", hadoopDir)
    val spark = SparkSession
      .builder()
      .appName("localTest")
      .master("local")
      .getOrCreate()
    spark
  }

  /** 获取sparkSession */
  def getSparkStreaming(spark: SparkSession, duration: Duration): StreamingContext = {
    StreamingContext.getActiveOrCreate(() => new StreamingContext(spark.sparkContext, duration))
  }
}
