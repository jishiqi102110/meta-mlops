package com.meta.entity

import com.meta.entity.FeatureDTO.FieldValue
import org.xerial.snappy.Snappy

object SerilizeUtils {

  def serialize(fieldValue: FieldValue, isCompress: Boolean): Array[Byte] = {
    if (isCompress) {
      Snappy.compress(fieldValue.toByteArray)
    } else {
      fieldValue.toByteArray
    }
  }

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
