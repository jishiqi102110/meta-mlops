package com.meta.spark.kafka

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import com.meta.featuremeta.RedisFloatMeta
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import java.time.Duration
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._ //scalastyle:ignore


class CtrStatStreaming(spark: SparkSession,
                       kafkaSource: KafkaSourceStreaming,
                       rediKeyPrefix: String,
                       jedisClusterNames: Array[JedisClusterName],
                       idFunArray: Array[(String, String => String)],
                       parseDstream: DStream[(String, String)] =>
                         DStream[(Int, String, Set[(String, String)])],
                       redisTTL: Int = 90 * 24 * 60 * 60,
                       alpha: Float = 0.9999f,
                       defaultShow: Float = 2000f,
                       defaultClick: Float = 10f,
                       defaultCtr: Float = 0.0f) extends Serializable {
  private var fisrtBatch = true
  private val redisCtrMetasMap = collection.mutable.Map.empty[(String, String), RedisFloatMeta]
  private val redisShowMetasMap = collection.mutable.Map.empty[(String, String), RedisFloatMeta]
  private val redisClickMetasMap = collection.mutable.Map.empty[(String, String), RedisFloatMeta]

  private val seqOp = null //(agg:ShowClickCountAggrator)
  private val comOp = null

  protected def saveCtr(spark: SparkSession,
                        events: DStream[(Int, String, Set[(String, String)])],
                        idFunArray: Array[(String, String => String)]
                        //kafkaSource
                       ): Unit = {
    events.foreachRDD {
      (rdd, time) =>
        if (fisrtBatch) {
          // 特征注册
          val ctrMetas = rdd.flatMap {
            case (_, _, condinInfo) =>
              condinInfo.map(_._1)
          }.distinct().collect().flatMap {
            condName => {
              idFunArray.map {
                case (cateName, _) =>
                  val redisKeyPattern = s"${rediKeyPrefix}_${cateName}_$condName:{$cateName}"
                  val fieldValue = FeatureDTO.FieldValue.newBuilder().
                    setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                    setValue(FeatureDTO.Value.newBuilder.setFloatVal(defaultClick)).
                    build()
                  val featureInfo = new RedisFloatMeta(jedisClusterNames(0), redisKeyPattern,
                    s"{$condName}", "streaming", fieldValue, FeatureTypeEnum.CROSS)
                  featureInfo.register()
                  ((cateName, condName), featureInfo)
              }
            }
          }.toMap
          redisCtrMetasMap ++= ctrMetas

          redisClickMetasMap ++= redisShowMetasMap.mapValues {
            redisMeta =>
              val redisKeyPattern = redisMeta.redisKeyPattern
              val fieldValue = FeatureDTO.FieldValue.newBuilder().
                setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                setValue(FeatureDTO.Value.newBuilder.setFloatVal(defaultClick)).
                build()
              new RedisFloatMeta(jedisClusterNames(0), s"click_$redisKeyPattern",
                redisMeta.redisField, "streaming", fieldValue, FeatureTypeEnum.ITEM)
          }

          redisShowMetasMap ++= redisCtrMetasMap.mapValues {
            redisMeta =>
              val redisKeyPattern = redisMeta.redisKeyPattern
              val fieldValue = FeatureDTO.FieldValue.newBuilder().
                setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                setValue(FeatureDTO.Value.newBuilder.setFloatVal(defaultShow)).
                build()
              new RedisFloatMeta(jedisClusterNames(0), s"show_$redisKeyPattern",
                redisMeta.redisField, "streaming", fieldValue, FeatureTypeEnum.ITEM)
          }

          for ((cateName, _) <- idFunArray) {
            val redisKeyPattern = s"${rediKeyPrefix}_$cateName:{$cateName}"
            val fieldValue = FeatureDTO.FieldValue.newBuilder().
              setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
              setValue(FeatureDTO.Value.newBuilder.setFloatVal(defaultCtr)).
              build()

            val featureInfo = new RedisFloatMeta(jedisClusterNames(0), redisKeyPattern,
              null, "", fieldValue, FeatureTypeEnum.ITEM)
            featureInfo.register()
          }
          fisrtBatch = false
        }

        val cacheRDD = rdd.map {
          case (clicked, cateInfo, condInfo) =>
            (cateInfo, (clicked, condInfo))
        } //.aggregateByKey()
        // todo 主体需要修改
        for ((cateName, idFun) <- idFunArray) {
        }
    }
  }

  def run(): Unit = {
    // todo 这里需要修改
    val ssc = StreamingContext.getActiveOrCreate(() => new StreamingContext(spark.sparkContext, kafkaSource.batchDuration))
    ssc.sparkContext.setLogLevel("ERROR")
    val partitionNum = spark.sparkContext.getConf.get("spark.executor.instances").toInt
    val kafkaStream = kafkaSource.getKafkaDStream(ssc).repartition(partitionNum)
    val events = parseDstream(kafkaStream).filter(_ != null)
    saveCtr(spark, events, idFunArray)
    ssc.start()
    ssc.awaitTermination()
  }
}
