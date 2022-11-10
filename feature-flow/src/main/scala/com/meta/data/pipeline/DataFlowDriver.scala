package com.meta.data.pipeline

import com.alibaba.fastjson.JSONObject
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.featuremeta.RedisFeatureMeta

import scala.collection.mutable

/**
 * 数据流操作主类，主要用来离线根据特征配置文件来获取特征
 *
 * <p>任何形式的特征获取、计算都可以使用此类进行使用，通过配置话的形式解决推荐算法数据流中特征从特征平台获取、处理、入库hbase形成样本的流程
 * 通过配置文件的形式解决推荐算法中经典的''线上线下''一致性处理问题
 * <p>此类中有一个优化点：如果配置文件配置了此特征需要缓存，这里还进行了item特征的获取优化，一次召回需要的item在获取的时候会存在重复查询
 * 因为一次访问对应的是1->n 的问题,这时候如果不进行item特征进行缓存，我们的特征获取服务就会存在重复读取的问题，对底层特征服务压力就很大，
 * 所以这里进行item特征进行缓存来优化读取防止redis存在热点问题
 * @author: weitaoliang
 * @version v1.0
 * */
class DataFlowDriver(val conf: DataFlowConfigReader) extends Serializable {
  private def getJsonOrEleseMap(key: String, jsonObject: JSONObject,
                                map: mutable.HashMap[String, Any]): Any = {
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
  def featureCollector(inputMap: mutable.HashMap[String, Any], json: JSONObject,
                       bufferMap: mutable.HashMap[String, Any]): Unit = {
    // 记录特征获取开始时间
    json.put("collect_feature_start_timeStamp", System.currentTimeMillis())
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
            RedisFeatureMeta.parseFeatureField(
              featureMeta.featureInfo.getFieldValueAny(key, field).get(field).get.
                asInstanceOf[FieldValue])))
        } else {
          json.put(featureMeta.featureName,
            RedisFeatureMeta.parseFeatureField(
              featureMeta.featureInfo.getFieldValueAny(key, field).get(field).get.
                asInstanceOf[FieldValue]))
        }
      } else if (!keyPlaceHolder.isEmpty && filedPlaceHolder.isEmpty) {
        // 如果特征需要缓存，则从缓存中操作
        if (featureMeta.isCache equals true) {
          json.put(featureMeta.featureName, bufferMap.getOrElseUpdate(featureMeta.featureName,
            RedisFeatureMeta.parseFeatureField(
              featureMeta.featureInfo.getAny(key).asInstanceOf[FieldValue])))
        } else { // 如果不缓存则从redis操作
          json.put(featureMeta.featureName, RedisFeatureMeta.parseFeatureField(
            featureMeta.featureInfo.getAny(key).
              asInstanceOf[FieldValue]))
        }
      } else {
        throw new Exception(s"feature collect exception ${featureMeta.featureName}!!!")
      }
    }
    // 2.根据配置文件进行特征转化
    featureTransform(inputMap, json)
    // 3.根据配置文件进行特征剔除
    excludeFeature(json)
    // 获取特征获取结束时间
    json.put("coolect_feature_end_timeStamp", System.currentTimeMillis())
  }


  // 特征转换处理
  private def featureTransform(
                                inputMap: mutable.HashMap[String, Any],
                                json: JSONObject
                              ): Unit = {
    for (transformer <- conf.transformers) {
      // 获取inputMap中特征处理需要的参数，最后合并参数，送给特征处理函数
      val params = transformer.featureParams.map(f =>
        (f, getJsonOrEleseMap(f, json, inputMap))).toMap
      // 转化后的特征名放入json中
      json.put(transformer.transformedName, transformer.eval(params))
    }
  }

  // 特征删除处理
  private def excludeFeature(json: JSONObject): Unit = {
    for (exclude <- conf.excludes) {
      json.remove(exclude)
    }
  }


}


object DataFlowDriver {
  def apply(conf: DataFlowConfigReader): DataFlowDriver = new DataFlowDriver(conf)
}
