package com.meta.data.conf

import com.meta.data.utils.MLUtils
import com.sun.org.apache.xml.internal.security.utils.XMLUtils

/**
 * 特征转换类，这里来定义用户自定义方法，通过方法映射进行特征处理
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class TransformerConf(val method: String,
                      val redisKeyPattern: String,
                      val redisField: String,
                      val featureParams: Array[String],
                      val ConstantParams: Array[String],
                      val transformedName: String) extends Serializable {

  // 预定义初始化方法
  private val _featureMethods: Map[String, MethodWrapper] = Map(
    "cos" -> new MethodWrapper("cros", MLUtils.cos _, alias = "1"),
    "log1p" -> new MethodWrapper("log1p", MLUtils.log1p _, alias = "2"),
    "normalization" -> new MethodWrapper("normlization", MLUtils.normalization _, "3"),
    "netTypeEncode" -> new MethodWrapper("netTypeEncode", MLUtils.netTypeEncode _, alias = "4"),
    "isHoliday" -> new MethodWrapper("isHoliday", MLUtils.md5 _, "5"),
    "hourScope" -> new MethodWrapper("hourScope", MLUtils.hourScope _, "6"),
    "nextDayIsHoliday" -> new MethodWrapper("nextDayisHoliday", MLUtils.nextDayIsHoliday _, "7"),
    "weekOfDay" -> new MethodWrapper("weekOfDay", MLUtils.weekOfDay _, "8"),
    "sparse" -> new MethodWrapper("sparse", MLUtils.weekOfDay _, "9"),
    "hash" -> new MethodWrapper("hash", MLUtils.hash _, "10"))

  /**
   * 函数触发函数，传入参数之后进行触发调用
   *
   * @Param [map] 方法名及其参数
   */
  def eval(map: Map[String, Any]): Any = {
    _featureMethods(method).eval(featureParams.map(n => map(n)) ++ ConstantParams)
  }
}
