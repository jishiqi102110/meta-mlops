package com.meta.data.conf

import com.meta.data.utils.MLUtils
import com.sun.org.apache.xml.internal.security.utils.XMLUtils

/**
 * description 
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class TransformerConf(val method: String,
                      val redisKeyPattern: String,
                      val redisField: String,
                      val featureParams: Array[String],
                      val ConstantParams: Array[String],
                      val transformedName: String
                     ) extends Serializable {


  private val _featureMethods: Map[String, MethodWrapper] = Map(
    "cos" -> new MethodWrapper(
      "cros",
      (x: Double) => MLUtils.cos(x),
      alias = "1"),
    "log1p" -> new MethodWrapper(
      "log1p",
      (x: Double, scale: String) => MLUtils.log1p(x, scale = scale.toInt),
      alias = "2"),
    "normalization" -> new MethodWrapper("normlization", null, "3"),
    "netTypeEncode" -> new MethodWrapper("netTypeEncode",
      (netType: String) => MLUtils.netTypeEncode(netType),
      alias = "4"),
    "isHoliday" -> new MethodWrapper("isHoliday", null, "5"),
    "hourScope" -> new MethodWrapper("hourScope", null, "6"),
    "nextDayisHoliday" -> new MethodWrapper("nextDayisHoliday", null, "7"),
    "weekOfDay" -> new MethodWrapper("weekOfDay", null, "8"),
    "sparse" -> new MethodWrapper("sparse", null, "9"),
    "hash" -> new MethodWrapper("hash", null, "10"))

  def eval(map: Map[String, Any]): Any = _featureMethods(method).eval(featureParams.map(n => map(n)) ++ ConstantParams)
}
