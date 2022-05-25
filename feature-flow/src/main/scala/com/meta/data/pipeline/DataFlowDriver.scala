package com.meta.data.pipeline

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable

/**
 * 数据流操作主类，主要用来离线根据特征配置文件来获取特征
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class DataFlowDriver private(val conf: DataFlowConfigReader) extends Serializable {
  private def getJsonOrEleseMap(key: String, jsonObject: JSONObject,
                                map: Map[String, Any]): Any = {
    if (jsonObject.containsKey(key)) {
      jsonObject.get(key)
    } else if (map != null && map.contains(key)) {
      map(key)
    } else {
      throw new Exception(s"key $key must be in json or map !!")
    }
  }

  // 根据配置文件，获取特征，最后放在json中返回,这里会根据特征获取keyplaceHolder 和fieldHolder 需要的值即上下文数据来获取特征
  // 最后将获取到的特征存入json中返回，最后供样本生成使用
  def featureCollector(inputMap: Map[String, Any], json: JSONObject,
                       bufferMap: mutable.HashMap[String, Any], isCache: Boolean): Unit = {
    // 记录特征获取开始时间
    json.put("coolect_feature_start_timeStamp", System.currentTimeMillis())
    // 1.根据配置文件获取特征
    for (featureMeta <- conf.featureMetas) {
      val keyPlaceHolder = featureMeta.featureInfo.keyPlaceHolder
      val filedPlaceHolder = featureMeta.featureInfo.fieldPlaceHolder
      val key = getJsonOrEleseMap(keyPlaceHolder.get, json, inputMap).toString
      // 获取特征从缓存或者redis查询,针对filedPlaceHolder不为空
      // 走两个路径分别去查询特征，keyPlaceHolder不会为空
      if (!keyPlaceHolder.isEmpty && !filedPlaceHolder.isEmpty) {
        val field = getJsonOrEleseMap(filedPlaceHolder.get, json, inputMap).toString
        // 如果特征需要缓存，则先从缓存中获取，如果缓存中没有则从redis获取，并更新缓存
        if (featureMeta.isCache equals true) {
          json.put(featureMeta.featureName, bufferMap.getOrElseUpdate(featureMeta.featureName,
            featureMeta.featureInfo.getFieldValueAny(key, field).get(field).get))
        } else {
          json.put(featureMeta.featureName,
            featureMeta.featureInfo.getFieldValueAny(key, field).get(field).get)
        }
      } else if (!keyPlaceHolder.isEmpty && filedPlaceHolder.isEmpty) {
        // 如果特征需要缓存，则从缓存中操作
        if (featureMeta.isCache equals true) {
          json.put(featureMeta.featureName, bufferMap.getOrElseUpdate(featureMeta.featureName,
            featureMeta.featureInfo.getAny(key)))
        } else { // 如果不缓存则从redis操作
          json.put(featureMeta.featureName, featureMeta.featureInfo.getAny(key))
        }
      } else {
        throw new Exception(s"feature collect execption ${featureMeta.featureName}!!!")
      }
    }
    // 2.根据配置文件进行特征转化
    for (transformer <- conf.transformers) {
      // 获取inputMap中特征处理需要的参数，最后合并参数，送给特征处理函数
      val params = transformer.featureParams.map(f =>
        (f, getJsonOrEleseMap(f, json, inputMap))).toMap
      // 转化后的特征名放入json中
      json.put(transformer.transformedName, transformer.eval(params))
    }
    // 3.根据配置文件进行特征剔除
    for (exclude <- conf.excludes) {
      json.remove(exclude)
    }
    // 获取特征获取结束时间
    json.put("coolect_feature_start_timeStamp", System.currentTimeMillis())
  }
}

object DataFlowDriver {
  def apply(conf: DataFlowConfigReader): DataFlowDriver = new DataFlowDriver(conf)
}
