package com.meta.pipeline

import com.alibaba.fastjson.JSONObject
import com.meta.Logging
import com.meta.data.pipeline.{DataFlowConfigReader, DataFlowDriver}
import com.meta.data.utils.{MLUtils, ReflectUtils}
import org.junit.Test

import scala.collection.mutable
import scala.reflect.runtime.universe

/**
 * testDataFlowConfigReader 测试类
 *
 * @author weitaoliang 
 * @version V1.0 
 */
class testDataFlowConfigReader extends Logging {

  @Test
  def testFlowDriver(): Unit = {
    // 这里用来测试读取配置文件流程，列举了如何进行配置特征、特征处理、特征删除相关配置,其中特征选取了我们在
    // feature-common 测试特征注册流程注册的特征 [[FeatureMetaUnitTest]]，用到这里的配置文件中，来模拟
    // 一次用户请求，用户传入上下文信息，然后根据注册特征、特征处理等配置即可进行特征获取流程
    val xmlFile = "featureflow.xml"
    val reader = DataFlowConfigReader.loadFromResources(xmlFile)
    logInfo(DataFlowConfigReader.toString)
    val flow = new DataFlowDriver(reader)

    // 模拟传入每次特征的上下文device_id
    val device_id = "device_id1" // 这里使用common里面注册的特征进行测试
    val groupid = "group1"

    val inputMap = new mutable.HashMap[String, Any]
    inputMap.put("device_id", device_id)
    inputMap.put("groupid", groupid)

    val json = new JSONObject()
    // 进行获取
    flow.featureCollector(inputMap, json, mutable.HashMap.empty[String, Any])
    // 打印特征获取
    logInfo(json.toJSONString)
    // 结果展示，就会
    // {"netTypeEncode":0,"coolect_feature_start_timeStamp":1667993694447,
    // "log1pVideoClickCtr":0.09531017980432493,"coolect_feature_end_timeStamp":1667993694595,
    // "age":0,"creativeID":"id1"}

  }

  @Test
  def testReflect(): Unit = {
    val log1p = ReflectUtils.reflectMethodInvoke(
      "com.meta.data.utils.MLUtils", "log1p", Array(3.0D, 1))
    logInfo(log1p.toString)
  }

  def testMethodWrapper(): Unit = {

  }


}
