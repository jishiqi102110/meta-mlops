package com.meta.spark.kafka

import com.meta.Logging
import com.meta.conn.redis.{JedisClusterName, JedisConnector}
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import com.meta.featuremeta.RedisFloatMeta
import com.meta.spark.monitor.SparkMonitor
import com.meta.utils.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

/**
 * ctr 交叉特征处理类，使用者可以直接传入需要处理的DStream,存储的key、集群信息、ctr alpha 衰败因子等默认参数即可，
 * ctr计算衰减因子，这样来避免点击、曝光数过低或者过高造成点击率很高或者很低的情况，具体算法论文地址可以找我私聊
 *
 * val newSaveTotalShow = oldShow * alpha + aggregator.totalShow
 * val newSaveTotalClick = oldClick * alpha + aggregator.totalClick
 * val ctr = (defaultClick + newSaveTotalClick) / (defaultShow + newSaveTotalShow)
 *
 * spark SparkSession
 * kafkaSource 传入[[KafkaSourceStreaming]]
 * redisKeyPrefix ctr交叉特征存储key前缀
 * jedisClusterName 存储的集群名称，用于初始化redis集群
 * idFunArray 获取id的方法数据，这种获取方法在离线sql特征入库中也有类似用法，用户传入id获取方法即可
 * parseDStream 用户传入将events数据流转换为ctr交叉类型需要的DStream形式的方法，因为每个业务数据不一样，所以都是用户自定义
 * redisTTL 特征过期时间
 * alpha 衰减因子
 *
 * @author: weitaoliang
 * @version v1.0
 * */

class CtrStatStreaming(spark: SparkSession,
                       kafkaSource: KafkaSourceStreaming,
                       redisKeyPrefix: String,
                       jedisClusterName: JedisClusterName,
                       idFunArray: Array[(String, String => String)],
                       parseDStream: DStream[(String, String)] =>
                         DStream[(Int, String, Set[(String, String)])],
                       redisTTL: Int = 90 * 24 * 60 * 60,
                       alpha: Float = 0.9999f,
                       defaultShow: Float = 2000f,
                       defaultClick: Float = 10f,
                       defaultCtr: Float = 0.0f) extends Serializable with Logging {

  // 首批数据标志
  private var firstBatch = true
  // 初始化点击特征meta Map
  private val redisCtrMetasMap = collection.mutable.Map.empty[(String, String), RedisFloatMeta]
  private val redisShowMetasMap = collection.mutable.Map.empty[(String, String), RedisFloatMeta]
  private val redisClickMetasMap = collection.mutable.Map.empty[(String, String), RedisFloatMeta]

  // ShowClickAggregator为自定义聚合方法类
  private val seqOp = (agg: ShowClickAggregator,
                       showClickInfo: (Int, Set[(String, String)])) => agg.add(showClickInfo)

  private val comOp = (agg1: ShowClickAggregator, agg2: ShowClickAggregator) => agg1.merge(agg2)

  /**
   * ctr特征处理
   *
   * @Param spark sparkSession
   * @Param events 用户传入的聚合点击曝光行为 DStream
   * @Param idFunArray 用户传入的获取metaName的方法数组，用于初始化特征
   */
  protected def saveCtr(spark: SparkSession,
                        events: DStream[(Int, String, Set[(String, String)])],
                        idFunArray: Array[(String, String => String)]): Unit = {


    events.foreachRDD {
      (rdd, time) =>
        if (firstBatch) {
          // 根据用户传入信息注册ctr特征
          initCtrMetaFeature(rdd, idFunArray)
          // 初始化曝光metaFeature
          initShowMetaFeature()
          // 初始化点击metaFeature
          initClickMetaFeature()
          firstBatch = false
        }
        // 生成点击曝光聚合rdd
        val showClickAggregatorRDD = generateShowClickAggregatorRDD(rdd)
        // 更新特征值
        updateFeature(showClickAggregatorRDD)
        // 加入监控
        SparkMonitor.synStreamingMonitor(spark, kafkaSource.groupid, time)
    }
  }


  def run(): Unit = {

    val ssc = MLUtils.getSparkStreaming(spark, kafkaSource.batchDuration)
    ssc.sparkContext.setLogLevel("ERROR")
    val partitionNum = spark.sparkContext.getConf.get("spark.executor.instances").toInt
    val kafkaStream = kafkaSource.getKafkaDStream(ssc).repartition(partitionNum)
    val events = parseDStream(kafkaStream).filter(_ != null)
    saveCtr(spark, events, idFunArray)
    ssc.start()
    ssc.awaitTermination()
  }

  /** 生成点击曝光聚合rdd */
  private def generateShowClickAggregatorRDD(rdd: RDD[(Int, String, Set[(String, String)])]
                                            ): RDD[(String, ShowClickAggregator)] = {
    rdd.map {
      case (clicked, cateInfo, condInfo) =>
        (cateInfo, (clicked, condInfo))
    }.aggregateByKey(new ShowClickAggregator)(seqOp, comOp)
  }

  /** 根据用户传入信息注册ctr特征 */
  private def initCtrMetaFeature(rdd: RDD[(Int, String, Set[(String, String)])],
                                 idFunArray: Array[(String, String => String)]): Unit = {

    // scalastyle:off
    val ctrMetas = rdd.flatMap {
      case (_, _, condinInfo) =>
        condinInfo.map(_._1)
    }.distinct().collect().flatMap {
      condName => {
        idFunArray.map {
          case (cateName, _) =>
            val redisKeyPattern = s"${redisKeyPrefix}_${cateName}_$condName:{$cateName}"
            val fieldValue = MLUtils.initFloatMeta(defaultClick)

            val featureInfo = RedisFloatMeta(jedisClusterName, redisKeyPattern,
              s"{$condName}", "streaming", fieldValue, FeatureTypeEnum.CROSS)

            // 进行特征注册
            featureInfo.register()
            ((cateName, condName), featureInfo)
        }
      }
    }.toMap
    // scalastyle:on

    redisCtrMetasMap ++= ctrMetas

    for ((cateName, _) <- idFunArray) {
      val redisKeyPattern = s"${redisKeyPrefix}_$cateName:{$cateName}"
      val fieldValue = MLUtils.initFloatMeta(defaultCtr)
      val featureInfo = new RedisFloatMeta(jedisClusterName, redisKeyPattern,
        null, "", fieldValue, FeatureTypeEnum.ITEM)
      featureInfo.register()
    }
  }

  /** 初始化曝光metaFeature */
  private def initShowMetaFeature(): Unit = {

    redisShowMetasMap ++= redisCtrMetasMap.mapValues {
      redisMeta =>
        val fieldValue = MLUtils.initFloatMeta(defaultShow)
        RedisFloatMeta(jedisClusterName, s"show_${redisMeta.redisKeyPattern}",
          redisMeta.redisField, "streaming", fieldValue, FeatureTypeEnum.ITEM)
    }
  }

  /** 初始化曝光metaFeature */
  private def initClickMetaFeature(): Unit = {
    redisClickMetasMap ++= redisShowMetasMap.mapValues {
      redisMeta =>
        val fieldValue = MLUtils.initFloatMeta(defaultClick)
        RedisFloatMeta(jedisClusterName, s"click_${redisMeta.redisKeyPattern}",
          redisMeta.redisField, "streaming", fieldValue, FeatureTypeEnum.ITEM)
    }
  }

  /** 更新ctr特征 */
  private def updateFeature(rdd: RDD[(String, ShowClickAggregator)]): Unit = {
    for ((cateName, idFun) <- idFunArray) {
      updateFeature(rdd, cateName)
    }
  }

  /** 更新ctr特征 */
  private def updateFeature(rdd: RDD[(String, ShowClickAggregator)], cateName: String): Unit = {
    rdd.foreach {
      case (cateID, aggregator) =>
        // 获取曝光key、点击key 然后拿到历史点击、曝光数据
        val jedis = JedisConnector(jedisClusterName)
        val redisKeyPattern = s"${redisKeyPrefix}_$cateName:{$cateID}"
        val showKey = s"show_$redisKeyPattern"
        val clickKey = s"click_$redisKeyPattern"

        // 历史曝光
        val oldShow = {
          val num = jedis.get(showKey)
          if (num == null) 0.0f else num.toFloat
        }
        // 历史点击
        val oldClick = {
          val num = jedis.get(clickKey)
          if (num == null) 0.0f else num.toFloat
        }

        // 计算新的点击曝光、ctr值
        val newSaveTotalShow = oldShow * alpha + aggregator.totalShow
        val newSaveTotalClick = oldClick * alpha + aggregator.totalClick
        val ctr = (defaultClick + newSaveTotalClick) / (defaultShow + newSaveTotalShow)

        // 更新点击值、曝光值、点击值
        jedis.set(redisKeyPattern, ctr.toString)
        jedis.expire(redisKeyPattern, redisTTL)

        jedis.set(showKey, newSaveTotalShow.toString)
        jedis.expire(showKey, redisTTL)
        jedis.set(clickKey, newSaveTotalClick.toString)
        jedis.expire(clickKey, redisTTL)
        aggregatorComputer(aggregator, cateName, cateID, newSaveTotalClick)
    }
  }

  /** 聚合计算 */
  private def aggregatorComputer(aggregator: ShowClickAggregator,
                                 cateName: String,
                                 cateID: String,
                                 newSaveTotalClick: Double
                                ): Unit = {
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
            // 计算新的所有cond汇总曝光值
            val newSaveCondTotalShow = value * alpha +
              aggregator.condShowMap.getOrElse((condName, k), 0)
            // 计算新的所有cond点击曝光值
            val newSaveCondTotalClick = saveCondTotalClick(k).getValue.getFloatVal * alpha +
              aggregator.condClickMap.getOrElse((condName, k), 0)
            val ctr = (defaultClick + newSaveTotalClick) / (defaultShow + newSaveCondTotalShow)
            (k, (newSaveCondTotalShow, newSaveCondTotalClick, ctr))
        }
        if (ctrs.nonEmpty) {
          updateALLFeature(ctrs,
            redisShowMeta,
            redisClickMeta,
            redisCtrMeta,
            cateID
          )
        }
    }
  }

  /** 更新所有特征 */
  private def updateALLFeature(map: Map[String, (Float, Float, Double)],
                               redisShowMeta: RedisFloatMeta,
                               redisClickMeta: RedisFloatMeta,
                               redisCtrMeta: RedisFloatMeta,
                               cateID: String
                              ): Unit = {

    val showMetas = map.mapValues {
      x =>
        FeatureDTO.FieldValue.newBuilder().
          setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
          setValue(FeatureDTO.Value.newBuilder.setFloatVal(x._1)).
          build()
    }
    redisShowMeta.saveField(cateID, showMetas)

    val clickMetas = map.mapValues {
      x =>
        FeatureDTO.FieldValue.newBuilder().
          setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
          setValue(FeatureDTO.Value.newBuilder.setFloatVal(x._2)).
          build()
    }
    redisClickMeta.saveField(cateID, clickMetas)

    val ctrMetas = map.mapValues {
      x =>
        FeatureDTO.FieldValue.newBuilder().
          setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
          setValue(FeatureDTO.Value.newBuilder.setFloatVal(x._3.toFloat)).
          build()
    }
    redisCtrMeta.saveField(cateID, ctrMetas)
    redisClickMeta.expire(cateID, redisTTL)
    redisShowMeta.expire(cateID, redisTTL)
    redisCtrMeta.expire(cateID, redisTTL)
  }
}


