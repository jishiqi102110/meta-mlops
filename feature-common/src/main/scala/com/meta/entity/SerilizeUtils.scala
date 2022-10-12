package com.meta.entity

import com.meta.entity.FeatureDTO.FieldValue
import org.xerial.snappy.Snappy

/**
 * 特征序列化工具类
 *
 * @author weitaoliang
 * @version V1.0
 * */

object SerilizeUtils {
  /**
   * 序列化工具
   *
   * @Param [fieldValue, isCompress]
   * @return Array[Byte] 返回序列化字节数组
   */
  def serialize(fieldValue: FieldValue, isCompress: Boolean): Array[Byte] = {

    if (isCompress) {
      Snappy.compress(fieldValue.toByteArray)
    } else {
      fieldValue.toByteArray
    }
  }

  /**
   * 反序列化工具
   *
   * @Param [bytes, defaultVal, isCompress]
   * @return FieldValue
   */
  def deserialize(bytes: Array[Byte], defaultVal: FieldValue, isCompress: Boolean): FieldValue = {
    if (bytes == null) {
      defaultVal
    }
    else {
      if (isCompress) {
        FieldValue.parseFrom(Snappy.uncompress(bytes))
      } else {
        FieldValue.parseFrom(bytes)
      }
    }
  }
}
