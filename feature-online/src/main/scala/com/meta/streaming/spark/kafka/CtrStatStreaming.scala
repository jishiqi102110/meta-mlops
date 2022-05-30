package com.meta.streaming.spark.kafka

import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import com.meta.featuremeta.RedisFloatMeta
import com.meta.streaming.spark.monitor.SparkMonitor
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream //scalastyle:ignore


class CtrStatStreaming(spark: SparkSession,
                       kafkaSource: KafkaSourceStreaming,
                       rediKeyPrefix: String,
                       jedisClusterName: JedisClusterName,
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
  private val seqOp = (agg: ShowClickAggregator,
                       showClickInfo: (Int, Set[(String, String)])
                      ) => agg.add(showClickInfo)

  private val comOp = (agg1: ShowClickAggregator, agg2: ShowClickAggregator
                      ) => agg1.merge(agg2)

  // ctr特征处理
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
                  val featureInfo = new RedisFloatMeta(jedisClusterName, redisKeyPattern,
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
              new RedisFloatMeta(jedisClusterName, s"click_$redisKeyPattern",
                redisMeta.redisField, "streaming", fieldValue, FeatureTypeEnum.ITEM)
          }

          redisShowMetasMap ++= redisCtrMetasMap.mapValues {
            redisMeta =>
              val redisKeyPattern = redisMeta.redisKeyPattern
              val fieldValue = FeatureDTO.FieldValue.newBuilder().
                setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                setValue(FeatureDTO.Value.newBuilder.setFloatVal(defaultShow)).
                build()
              new RedisFloatMeta(jedisClusterName, s"show_$redisKeyPattern",
                redisMeta.redisField, "streaming", fieldValue, FeatureTypeEnum.ITEM)
          }

          for ((cateName, _) <- idFunArray) {
            val redisKeyPattern = s"${rediKeyPrefix}_$cateName:{$cateName}"
            val fieldValue = FeatureDTO.FieldValue.newBuilder().
              setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
              setValue(FeatureDTO.Value.newBuilder.setFloatVal(defaultCtr)).
              build()

            val featureInfo = new RedisFloatMeta(jedisClusterName, redisKeyPattern,
              null, "", fieldValue, FeatureTypeEnum.ITEM)
            featureInfo.register()
          }
          fisrtBatch = false
        }

        val cacheRDD = rdd.map {
          case (clicked, cateInfo, condInfo) =>
            (cateInfo, (clicked, condInfo))
        }.aggregateByKey(new ShowClickAggregator)(seqOp, comOp)
        for ((cateName, idFun) <- idFunArray) {
          cacheRDD.map {
            case (id, agg) =>
              (idFun(id), agg)
          }.reduceByKey(_ merge _).foreach {
            case (cateID, aggregator) =>
              val jedis = JedisConnector(jedisClusterName)
              val redisKeyPattern = s"${rediKeyPrefix}_$cateName:{$cateID}"
              val showKey = s"show_$redisKeyPattern"
              val clickKey = s"click_$redisKeyPattern"

              val oldShow = {
                val num = jedis.get(showKey)
                if (num == null) 0.0f else num.toFloat
              }
              val oldClick = {
                val num = jedis.get(clickKey)
                if (num == null) 0.0f else num.toFloat
              }

              val newSaveTotalShow = oldShow * alpha + aggregator.totalShow
              val newSaveTotalClick = oldClick * alpha + aggregator.totalClick

              val ctr = (defaultClick + newSaveTotalClick) / (defaultShow + newSaveTotalShow)
              // todo 修复
              jedis.set(redisKeyPattern, ctr.toString)
              jedis.expire(redisKeyPattern, redisTTL)

              jedis.set(showKey, newSaveTotalShow.toString)
              jedis.expire(showKey, redisTTL)
              jedis.set(clickKey, newSaveTotalClick.toString)
              jedis.expire(clickKey, redisTTL)
              (aggregator.condClickMap.keySet ++ aggregator.condShowMap.keySet).groupBy(_._1).foreach {
                case (condName, iter) =>
                  val condIDs = iter.map(_._2).toArray
                  val redisCtrMeta = redisCtrMetasMap(cateName, condName)
                  val redisClickMeta = redisClickMetasMap(cateName, condName)
                  val redisShowMeta = redisShowMetasMap(cateName, condName)
                  val saveCondTotalShow = redisShowMeta.getFieldValue(cateID, condIDs: _*)
                  val saveCondTotalClick = redisClickMeta.getFieldValue(cateID, condIDs: _*)
                  val ctrs = saveCondTotalShow.map {
                    case (k, v) =>
                      val value = v.getValue.getFloatVal
                      val newSaveCondTotalShow = value * alpha + aggregator.condShowMap.getOrElse((condName, k), 0)
                      val newSaveCondTotalClick = saveCondTotalClick(k).getValue.getFloatVal * alpha + aggregator.condClickMap.getOrElse((condName, k), 0)
                      val ctr = (defaultClick + newSaveTotalClick) / (defaultShow + newSaveCondTotalShow)
                      (k, (newSaveCondTotalShow, newSaveCondTotalClick, ctr))
                  }
                  if (ctrs.nonEmpty) {
                    val showMetas = ctrs.mapValues {
                      x =>
                        FeatureDTO.FieldValue.newBuilder().
                          setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                          setValue(FeatureDTO.Value.newBuilder.setFloatVal(x._1)).
                          build()
                    }
                    redisShowMeta.saveField(cateID, showMetas)

                    val clickMetas = ctrs.mapValues {
                      x =>
                        FeatureDTO.FieldValue.newBuilder().
                          setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                          setValue(FeatureDTO.Value.newBuilder.setFloatVal(x._2)).
                          build()
                    }
                    redisClickMeta.saveField(cateID, clickMetas)

                    val ctrMetas = ctrs.mapValues {
                      x =>
                        FeatureDTO.FieldValue.newBuilder().
                          setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                          setValue(FeatureDTO.Value.newBuilder.setFloatVal(x._3)).
                          build()
                    }
                    redisCtrMeta.saveField(cateID, ctrMetas)

                    redisClickMeta.expire(cateID, redisTTL)
                    redisShowMeta.expire(cateID, redisTTL)
                    redisCtrMeta.expire(cateID, redisTTL)
                  }
              }


          }
        }
        SparkMonitor.synStreamingMonitor(spark, kafkaSource.groupid, time)
    }
  }

  def run(): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(
      () => new StreamingContext(spark.sparkContext, kafkaSource.batchDuration))
    ssc.sparkContext.setLogLevel("ERROR")
    val partitionNum = spark.sparkContext.getConf.get("spark.executor.instances").toInt
    val kafkaStream = kafkaSource.getKafkaDStream(ssc).repartition(partitionNum)
    val events = parseDstream(kafkaStream).filter(_ != null)
    saveCtr(spark, events, idFunArray)
    ssc.start()
    ssc.awaitTermination()
  }
}
