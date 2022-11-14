package com.meta.data.utils

import java.security.MessageDigest

import com.meta.Logging
import com.meta.entity.FeatureDTO
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureDTO.FieldValue.ValueType

/**
 * 机器学习工具类
 *
 * @author: weitaoliang
 * @version v1.0
 * */
object MLUtils {

  /**
   * log1p
   *
   * @Param [x, scale]
   * @return
   */
  def log1p(x: Double, scale: String = "1"): Double = {
    if (x == 0.0) 0.0 else (if (x >= 0) 1 else -1) * Math.log(Math.abs(x) / scale.toDouble + 1.0)
  }

  /**
   * cos
   *
   * @Param [x]
   * @return
   */
  def cos(x: Double): Double = {
    Math.cos(x)
  }

  /**
   * md5 加密算法
   *
   * @Param [s]
   * @return
   */
  def md5(s: String): String = {
    val ret = MessageDigest.getInstance("MD5").
      digest(s.getBytes()).
      map("%02x".format(_)).
      foldLeft("") {
        _ + _
      }
    ret
  }

  /**
   * sha1 加密算法
   *
   * @Param [s]
   * @return
   */
  def sha1(s: String): String = {
    val digest = MessageDigest.getInstance("SHA-1")
    digest.update(s.getBytes)
    val messageDigest = digest.digest()
    val hexString = new StringBuilder
    var i = 0
    while ( {
      i < messageDigest.length

    }) {
      val shaHex = Integer.toHexString(messageDigest(i) & 0xFF)
      if (shaHex.length < 2) hexString.append(0)
      hexString.append(shaHex) {
        i += 1;
        i - 1
      }
    }
    hexString.toString().toUpperCase()
  }


  /**
   * netWork编码器
   *
   * @Param [netType] 网络类型
   * @return
   */
  // scalastyle:off

  def netTypeEncode(netType: String): Int = {
    if (netType != null && "".equals(netType)) {
      netType match {
        case FlowConstant.NET_WORK_TYPE_1 => 1
        case FlowConstant.NET_WORK_TYPE_2 => 2
        case FlowConstant.NET_WORK_TYPE_3 => 3
        case FlowConstant.NET_WORK_TYPE_4 => 4
        case FlowConstant.NET_WORK_TYPE_5 => 5
        case FlowConstant.NET_WORK_TYPE_6 => 6
        case FlowConstant.NET_WORK_TYPE_7 => 7
        case FlowConstant.NET_WORK_TYPE_8 => 8
        case _ => 0
      }
    } else {
      0
    }
  }


  /**
   * 年龄分桶工具
   *
   * @Param [age]
   * @return
   */
  def ageBucket(age: Int): Int = {
    val bucketValue = if (age >= 1 && age <= 11) {
      1
    } else if (age >= 12 && age <= 14) {
      2
    } else if (age >= 15 && age <= 17) {
      3
    } else if (age >= 18 && age <= 21) {
      4
    } else if (age >= 22 && age <= 24) {
      5
    } else if (age >= 25 && age <= 29) {
      6
    } else if (age >= 30 && age <= 34) {
      7
    } else if (age >= 35 && age <= 39) {
      8
    } else if (age >= 40 && age <= 49) {
      9
    } else if (age >= 50 && age <= 99) {
      10
    } else {
      0
    }
    bucketValue
  }
  // scalastyle:on

  def normalization(): Unit = {}

  def isHoliday(): Unit = {}

  def hourScope(): Unit = {}

  def nextDayIsHoliday(): Unit = {}

  def weekOfDay(): Unit = {}

  def sparse(): Unit = {}

  def hash(): Unit = {}

}

