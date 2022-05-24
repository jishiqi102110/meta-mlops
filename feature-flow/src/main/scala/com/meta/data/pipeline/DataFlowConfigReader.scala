package com.meta.data.pipeline

import com.alibaba.fastjson.JSONObject
import com.meta.conn.hbase.HbaseConnectInfo
import com.meta.conn.redis.JedisClusterName
import com.meta.data.conf.{HbaseInfoConfig, RedisFeatureMetaWrapper, TFFeatureConfig, TransformerConf}
import com.meta.entity.{FeatureDTO, FeatureTypeEnum, RedisEnum}
import com.meta.featuremeta.{RedisIntMeta, RedisSeqListMeta}

import scala.collection.mutable.ArrayBuffer
import scala.xml.Elem

/**
 * 数据流配置文件读取类
 *
 * @author: weitaoliang
 * @version v1.0
 * */

class DataFlowConfigReader extends Serializable {
  // 配置文件中的集群配置
  private var _redisNames: Map[String, JedisClusterName] = _
  // 配置文件中的meta信息
  private var _featureMetas: Array[RedisFeatureMetaWrapper] = _
  // 配置中心中的特征转化配置
  private var _transformers: Array[TransformerConf] = _
  // hbase配置
  private var _hbaseConfig: HbaseInfoConfig = _
  // tfRecord 配置
  private var _tfRecordConfig: Array[TFFeatureConfig] = _

  // 给featureMetas 排序，没有前置依赖的特征排在前面
  private def sortFeatureMeta(featureMetas: Array[RedisFeatureMetaWrapper]
                             ): Array[RedisFeatureMetaWrapper] = {
    val featureNameSet = {
      // 根据特证名聚合，检查是否有重复特征名，如果有throw异常
      val featureNameGroup = featureMetas.map(_.featureName).groupBy(x => x)
      val illegalFeatureNames = featureNameGroup.filter(_._2.length > 1)
      if (illegalFeatureNames.nonEmpty) {
        throw new Exception(s"illegal featureName ${illegalFeatureNames.keys.mkString("|")}")
      }
      featureNameGroup.keySet
    }
    val sortedArray = new ArrayBuffer[RedisFeatureMetaWrapper]()
    val unSortedArray = new ArrayBuffer[RedisFeatureMetaWrapper]()
    unSortedArray ++= featureMetas
    var count = 0
    var flag = true
    while (flag) {
      val readyFeatures = unSortedArray.filter {
        feature =>
          val sortedKey = sortedArray.map(_.featureName).toSet union Set.empty[String]
          // 特征获取没有前置特征获取需求的放在sortedArray中
          if (!featureNameSet.contains(feature.featureInfo.keyPlaceHolder.get)
            && !featureNameSet.contains(feature.featureInfo.fieldPlaceHolder.get)) {
            true
          } else if (sortedKey.contains(feature.featureInfo.keyPlaceHolder.get)
            && sortedKey.contains(feature.featureInfo.fieldPlaceHolder.get)) {
            true
          } else {
            false
          }
      }
      if (count == readyFeatures.length) {
        sortedArray ++= unSortedArray
        flag = false
      } else {
        count = readyFeatures.length
      }
      unSortedArray --= readyFeatures
      sortedArray ++= readyFeatures
    }
    sortedArray.toArray
  }

  // 从xml文件中加载配置
  private def loadFromXml(elem: Elem): Unit = {
    _redisNames = (elem \ "redisInfos" \ "redisInfo").map {
      redisInfoNode =>
        // 提取xml文件redis 相关配置
        val redisName = (redisInfoNode \ "redisName").text
        val redisAddress = (redisInfoNode \ "redisAddress").text
        val port = (redisInfoNode \ "port").text
        val auth = (redisInfoNode \ "auth").text
        val redisType = (redisInfoNode \ "redisType").text
        val redisEnum = if (redisType.equals("redis")) {
          RedisEnum.REDIS
        } else {
          RedisEnum.SSD_REDIS
        }

        val timeOut = (redisInfoNode \ "timeOut").text
        (redisName, new JedisClusterName(redisName, redisAddress, port.toInt, auth = auth,
          timeout = timeOut.toInt, redisType = redisEnum))
    }.toMap

    // 提取xml meta相关配置
    _featureMetas = (elem \ "featureMetas" \ "featureMeta").map {
      featureMetaNode =>
        // 提取xml文件featureMeta相关配置
        val redisKeyPattern = (featureMetaNode \ "redisKeyPattern").text
        val redisField = (featureMetaNode \ "redisField").text
        val className = (featureMetaNode \ "className").text
        val redisName = (featureMetaNode \ "redisName").text
        val isCache = (featureMetaNode \ "isCache").text match {
          case "false" => false
          case "true" => true
        }
        val defalutVal = (featureMetaNode \ "defaultVal").text
        val dataSource = (featureMetaNode \ "dataSource").text
        val featureType = (featureMetaNode \ "featureType").text match {
          case "cross" => FeatureTypeEnum.CROSS
          case "user" => FeatureTypeEnum.USER
          case "item" => FeatureTypeEnum.ITEM
          case "scene" => FeatureTypeEnum.SCENE
        }
        val feaureMeta = className match {
          case "RedisFloatMeta" =>
            val floatDefaultVal = FeatureDTO.FieldValue.newBuilder()
              .setValueType(FeatureDTO.FieldValue.ValueType.FLOAT)
              .setValue(FeatureDTO.Value.newBuilder.setFloatVal(defalutVal.toFloat))
              .build()
            new RedisIntMeta(_redisNames(redisName), redisKeyPattern, redisField,
              dataSource, floatDefaultVal, featureType)

          case "RedisIntMeta" =>
            val intDefaultVal = FeatureDTO.FieldValue.newBuilder()
              .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
              .setValue(FeatureDTO.Value.newBuilder.setInt32Val(defalutVal.toInt))
              .build()
            new RedisIntMeta(_redisNames(redisName), redisKeyPattern, redisField,
              dataSource, intDefaultVal, featureType)
          case "RedisStringMeta" =>
            val stringDefaultVal = FeatureDTO.FieldValue.newBuilder()
              .setValueType(FeatureDTO.FieldValue.ValueType.STRING)
              .setValue(FeatureDTO.Value.newBuilder.setStringVal(defalutVal))
              .build()
            new RedisIntMeta(_redisNames(redisName), redisKeyPattern, redisField,
              dataSource, stringDefaultVal, featureType)
          case "RedisSeqListMeta" =>
            val stringDefaultVal = FeatureDTO.FieldValue.newBuilder()
              .setValueType(FeatureDTO.FieldValue.ValueType.STRING)
              .setValue(FeatureDTO.Value.newBuilder.setStringVal(defalutVal))
              .build()
            new RedisSeqListMeta(_redisNames(redisName), redisKeyPattern, redisField,
              dataSource, stringDefaultVal, featureType)
        }
        new RedisFeatureMetaWrapper(redisField, feaureMeta, isCache)
    }.toArray
    // 进行排序
    _featureMetas = sortFeatureMeta(_featureMetas)
    // 提取xml hbase相关配置

    _hbaseConfig = {
      val clusterName = (elem \ "hbaseInfo" \ "clusterName").text
      val zookeeperQuorum = (elem \ "hbaseInfo" \ "zookeeperQuorum").text
      val port = (elem \ "hbaseInfo" \ "port").text
      val tableName = (elem \ "hbaseInfo" \ "tableName").text
      val ttl = (elem \ "hbaseInfo" \ "ttl").text
      new HbaseInfoConfig(new HbaseConnectInfo(clusterName, zookeeperQuorum, port),
        tableName, ttl.toLong)
    }
    // 提取转化操作
    _transformers = (elem \ "transformers" \ "transformer").map {
      transformer =>
        // 提取方法
        val method = (transformer \ "operator").text
        // 提取参数
        val params = (transformer \ "params" \ "param").map(_.text).toArray
        // 提取常量参数
        val constantParams = (transformer \ "params" \ "constantParam").map(_.text).toArray
        // 提取需要处理的特征id
        val featureName = (transformer \ "featureName").text
        val transformedFeatureName = (transformer \ "transformedFeatureName").text
        new TransformerConf(method, featureName, params, constantParams, transformedFeatureName)
    }.toArray
  }

  // 从resource目录下读取
  def loadFromResources(fileName: String): DataFlowConfigReader = {
    val in = getClass.getClassLoader.getResourceAsStream(fileName)
    val elem = scala.xml.XML.load(in)
    loadFromXml(elem)
    this
  }

  // 从文件目录下读取
  def loadFromFile(path: String): DataFlowConfigReader = {
    val elem = scala.xml.XML.load(path)
    loadFromXml(elem)
    this
  }

  def featureMetas: Array[RedisFeatureMetaWrapper] = _featureMetas

  def redisNames: Map[String, JedisClusterName] = _redisNames

  def transformers: Array[TransformerConf] = _transformers

  def hbaseConfig: HbaseInfoConfig = _hbaseConfig

  def tfRecordConfig: Array[TFFeatureConfig] = _tfRecordConfig

}