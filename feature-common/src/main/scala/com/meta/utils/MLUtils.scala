package com.meta.utils

import com.meta.Logging
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil, TableDesc}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 机器学习工具类
 *
 * @author weitaoliang
 * @version V1.0
 */
object MLUtils extends Logging {


  ///////////////////////////////////////////////////////////////////////////
  // spark operations
  ///////////////////////////////////////////////////////////////////////////

  // 获取sparkSession
  def getSparkSession(appName: String): SparkSession = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()
    spark
  }

  // 获取本地SparkSession,用于本地调试，注意这里仅适用于windows
  def getLocalSparkSession(appName: String, hadoopDir: String): SparkSession = {
    System.setProperty("HADOOP_HOME", hadoopDir)
    val spark = SparkSession
      .builder()
      .appName("localTest")
      .master("local")
      .getOrCreate()
    spark
  }

  ///////////////////////////////////////////////////////////////////////////
  // tdw operations
  ///////////////////////////////////////////////////////////////////////////

  /** split table name */
  private def splitDbTableName(dbTableName: String): (String, String) = {
    val parts = dbTableName.split("::")
    val nPart = parts.size
    assert(nPart == 2 || nPart == 1, s"Invalid param:$dbTableName")
    val (dbName, tableName) = (parts(0), parts(1))
    (dbName, tableName)
  }

  /** tdw建表方法 */
  def tdwTable(dbTableName: String, priParts: Seq[String], subParts: Seq[String],
               limit: Int, spark: SparkSession): DataFrame = {
    val (dbName, tableName) = splitDbTableName(dbTableName)
    tdwTable(dbName, tableName, priParts, subParts, limit, spark)
  }

  /** tdw建表方法 */
  def tdwTable(dbName: String, tableName: String, priParts: Seq[String], subParts: Seq[String],
               limit: Int, spark: SparkSession): DataFrame = {
    logInfo(s"tdwTable($dbName, $tableName, " +
      s"limit=$limit, priParts=${priParts.mkString(",")})," +
      s" subParts=${subParts.mkString(",")}")
    if (limit > 0) {
      new TDWSQLProvider(spark, dbName).table(tableName, priParts, subParts).limit(limit)
    } else {
      new TDWSQLProvider(spark, dbName).table(tableName, priParts, subParts)
    }
  }

  /** tdw建表方法 */
  def createTable(dbTableName: String, cols: Seq[Array[String]], comment: String,
                  partType: String, partField: String,
                  subPartType: String, subPartField: String, overwrite: Boolean): Unit = {
    val (dbName, tableName) = splitDbTableName(dbTableName)
    val tableDesc = new TableDesc()
    tableDesc.
      setTblName(tableName).
      setCols(cols).
      setComment(comment).
      setPartType(partType).
      setPartField(partField).
      setSubPartType(subPartType).
      setSubPartField(subPartField)
    val tdwUtil = new TDWUtil(dbName)
    if (!tdwUtil.tableExist(tableName) || overwrite) {
      logInfo(s"Creating table $dbTableName ...")
      tdwUtil.createTable(tableDesc)
    }
  }

  /** tdw写入方法 */
  def writeTable(df: DataFrame, dbTableName: String,
                 priPartName: String, priPart: String,
                 subPartName: String, subPart: String,
                 spark: SparkSession): Unit = {
    val (dbName, tableName) = splitDbTableName(dbTableName)
    logInfo(f"writeTable: $dbName::$tableName $priPartName:$priPart $subPartName:$subPart")
    val tdwUtil = new TDWUtil(dbName)
    if (!tdwUtil.tableExist(tableName)) {
      print(f"  Table not exists. Create table manually!\n")
    } else {
      if (!tdwUtil.partitionExist(tableName, priPartName)) {
        tdwUtil.createListPartition(tableName, priPartName, priPart, 0)
      }
      if (!tdwUtil.partitionExist(tableName, subPartName)) {
        tdwUtil.createListPartition(tableName, subPartName, subPart, 1)
      }
      logInfo(f"  Appending table ...")
      new TDWSQLProvider(spark, dbName).saveToTable(df, tableName, priPartName, subPartName, true)
      logInfo(f"  Finish")
    }
  }

  /** tdw写入方法 */
  def writeTable(df: DataFrame, dbTableName: String,
                 priPartName: String, priPart: String,
                 overwrite: Boolean, spark: SparkSession): Unit = {
    val (dbName, tableName) = splitDbTableName(dbTableName)
    logInfo(f"writeTable: $dbName::$tableName $priPartName:$priPart")
    val tdwUtil = new TDWUtil(dbName)
    if (!tdwUtil.tableExist(tableName)) {
      logInfo(f"  Table not exists. Create table manually!")
    } else {
      if (overwrite && tdwUtil.partitionExist(tableName, priPartName)) {
        tdwUtil.dropPartition(tableName, priPartName, 0)
      }
      if (!tdwUtil.partitionExist(tableName, priPartName)) {
        tdwUtil.createListPartition(tableName, priPartName, priPart, 0)
      }
      logInfo(f"  Appending table ...")
      new TDWSQLProvider(spark, dbName).saveToTable(df, tableName, priPartName, overwrite)
      logInfo(f"  Finish.")
    }
  }

  /** tdw写入方法 */
  def writeTable(df: DataFrame, dbTableName: String, spark: SparkSession): Unit = {
    val (dbName, tableName) = splitDbTableName(dbTableName)
    logInfo(f"Write table: $dbName::$tableName")
    val tdwUtil = new TDWUtil(dbName)
    if (tdwUtil.tableExist(tableName)) {
      logInfo(f"Drop table already exists")
      tdwUtil.dropTable(tableName)
    }
    logInfo(f"Creating table ...")
    tdwUtil.createTable(tableName, df.schema)
    logInfo(f"Writing table...")
    new TDWSQLProvider(spark, dbName).saveToTable(df, tableName)
    logInfo(f"Finish")
  }


  ///////////////////////////////////////////////////////////////////////////
  // hdfs operations
  ///////////////////////////////////////////////////////////////////////////

  /** 合并列 */
  def concatCols(df: DataFrame, col1: String, col2: String, sep: String): DataFrame = {
    df.select(concat_ws(sep, df(col1), df(col2)))
  }

  /** 是否存在hdfs地址 */
  def existHdfs(path: String, spark: SparkSession, verbose: Boolean = false): Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration
    val ret = FileSystem.get(conf).exists(new Path(path))
    if (verbose) {
      logInfo("Set conf to avoid error: spark.hadoop.fs.defaultFS=hdfs://qy-ieg-4-v2/\n")
      logInfo(s"existHdfs $ret: $path\n")
    }
    ret
  }

  /** 创建hdfs地址 */
  def mkdirHdfs(path: String, spark: SparkSession): Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration
    val ret = FileSystem.get(conf).mkdirs(new Path(path))
    logInfo(s"mkdirHdfs $ret: $path")
    ret
  }

  /** 判断是否是hdfs地址 */
  def isDirHdfs(path: String, spark: SparkSession): Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration
    val ret = FileSystem.get(conf).isDirectory(new Path(path))
    logInfo(s"isDirHdfs $ret: $path")
    ret
  }

  /** 读取hdfs地址数据 */
  def readHdfs(path: String, spark: SparkSession): DataFrame = {
    spark.sqlContext.read.
      format("com.databricks.spark.csv").
      option("delimiter", ",").
      option("header", "true").
      load(path)
  }

  /** 写入hdfs */
  def writeHdfs(df: DataFrame, path: String, delimiter: String,
                header: Boolean, spark: SparkSession): Unit = {
    writeHdfs(df, path, delimiter, header, "\"", "NON_NUMERIC", spark)
  }

  /** 写入hdfs */
  def writeHdfs(df: DataFrame, path: String, delimiter: String,
                header: Boolean, quote: String, quoteMode: String,
                spark: SparkSession): Unit = {
    logInfo(
      s"""
         |Writing $path partitions:${df.rdd.getNumPartitions} ...
         |Set spark-conf to avoid permission denied:
         |  spark.hadoop.hadoop.job.ugi=tdw_richliu:passwd,g_ieg_iegpdata_ieg_mms\n""".stripMargin)
    val headerStr = if (header) {
      "true"
    } else {
      "false"
    }
    df.write.
      format("com.databricks.spark.csv").
      option("header", headerStr).
      option("delimiter", delimiter).
      option("quote", quote).
      option("quoteMode", quoteMode).
      mode("overwrite").
      save(path)
  }

  /** libsvm写入hdfs */
  def writeLibsvm2Hdfs(df: DataFrame, path: String, spark: SparkSession): Unit = {
    print(
      s"""
         |Writing $path ...
         |Set spark-conf to avoid permission denied:
         |  spark.hadoop.hadoop.job.ugi=tdw_richliu:passwd,g_ieg_iegpdata_ieg_mms\n""".stripMargin)
    df.write.
      format("libsvm").
      option("header", "false").
      mode("overwrite").
      save(path)
  }


}
