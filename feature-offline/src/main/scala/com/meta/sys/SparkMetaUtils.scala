package com.meta.sys

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import com.meta.featuremeta.{RedisFeatureInfo, RedisFloatMeta, RedisIntMeta, RedisSeqListMeta, RedisStringMeta}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}


/**
 * spark metautils 将hive数据解析为meta元数据并获取到特征序列化值
 *
 * @author weitaoliang
 * @version V1.0
 * */

object SparkMetaUtils {

  // scalastyle:off
  // method longer than 50 lines
  // 这里去获得每个特征对应原始信息及如何从row 拿到特征序列化的值
  private def featureMetasAndGetFeatureMethods(df: DataFrame,
                                               redisKeyPattern: String,
                                               jedisClusterName: JedisClusterName,
                                               defaultValues: Map[String, Any],
                                               dataSource: String): Seq[(RedisFeatureInfo, Row => Array[Byte])] = {
    val _featureMetasAndGetFeatureMethods: Seq[(RedisFeatureInfo, Row => Array[Byte])] = {
      df.schema.fields.map {
        field =>
          val fieldName = field.name
          val meta = field.dataType match {

            case IntegerType =>
              val intDefaultVal = FeatureDTO.FieldValue.newBuilder()
                .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
                .setValue(FeatureDTO.Value.newBuilder
                  .setInt32Val(defaultValues.getOrElse(fieldName, 0).toString.toInt))
                .build()
              new RedisIntMeta(jedisClusterName, redisKeyPattern, fieldName,
                dataSource, intDefaultVal, featureType(redisKeyPattern, fieldName))

            case DoubleType | FloatType => // 统一采用float精度减少内存使用
              val floatDefaultVal = FeatureDTO.FieldValue.newBuilder()
                .setValueType(FeatureDTO.FieldValue.ValueType.FLOAT)
                .setValue(FeatureDTO.Value.newBuilder
                  .setFloatVal(defaultValues.getOrElse(fieldName, 0.0).toString.toFloat))
                .build()
              new RedisFloatMeta(jedisClusterName, redisKeyPattern, fieldName,
                dataSource, floatDefaultVal, featureType(redisKeyPattern, fieldName))

            case StringType =>
              val StringDefaultVal = FeatureDTO.FieldValue.newBuilder()
                .setValueType(FeatureDTO.FieldValue.ValueType.STRING)
                .setValue(FeatureDTO.Value.newBuilder
                  .setStringVal(defaultValues.getOrElse(fieldName, 0).toString))
                .build()
              // 这里seq_list 时间序列类型的特征列名必须按照 time_seq_ 为前缀
              if (fieldName.startsWith("time_seq_")) {
                new RedisSeqListMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource,
                  StringDefaultVal, featureType(redisKeyPattern, fieldName))
              } else {
                new RedisStringMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource, true, StringDefaultVal, featureType(redisKeyPattern, fieldName))
              }
              // todo 后续再添加Map List 等复合类型
            //case map: MapType =>
            //  case _ =>
          }
          val getFeature: Row => Array[Byte] = field.dataType match {

            case IntegerType => row => {
              val value = row.getAs[Integer](fieldName)
              val fieldValue = FeatureDTO.FieldValue.newBuilder()
                .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
                .setValue(FeatureDTO.Value.newBuilder.setInt32Val(value))
                .build()
              meta.serialize(fieldValue)
            }

            case StringType => row => {
              val value = row.getAs[String](fieldName)
              val fieldValue = FeatureDTO.FieldValue.newBuilder().
                setValueType(FeatureDTO.FieldValue.ValueType.STRING).
                setValue(FeatureDTO.Value.newBuilder.setStringVal(value)).
                build()
              meta.serialize(fieldValue)
            }

            case DoubleType => row => {
              val value = row.getAs[Double](fieldName)
              val fieldValue = FeatureDTO.FieldValue.newBuilder().
                setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                setValue(FeatureDTO.Value.newBuilder.setFloatVal(value.toFloat)).
                build()
              meta.serialize(fieldValue)
            }

            case FloatType => row => {
              val value = row.getAs[Float](fieldName)
              val fieldValue = FeatureDTO.FieldValue.newBuilder().
                setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                setValue(FeatureDTO.Value.newBuilder.setFloatVal(value)).
                build()
              meta.serialize(fieldValue)
            }

          }
          (meta, getFeature)
      }
    }
    _featureMetasAndGetFeatureMethods
  }
  // method longer than 50 lines
  // scalastyle:on

  def schemaMetas(df: DataFrame,
                  redisKeyPattern: String,
                  jedisClusterName: JedisClusterName,
                  defaultValues: Map[String, Any],
                  dataSource: String): Seq[(RedisFeatureInfo, Row => Array[Byte])] = {

    val _featureMetasAndGetFeatureMethods: Seq[(RedisFeatureInfo, Row => Array[Byte])] =
      featureMetasAndGetFeatureMethods(
        df,
        redisKeyPattern,
        jedisClusterName,
        defaultValues,
        dataSource)
    _featureMetasAndGetFeatureMethods
  }


  private def featureType(redisKeyPattern: String,
                          redisField: String): FeatureTypeEnum = {
    if (redisField != null && redisField.contains("{")) {
      FeatureTypeEnum.CROSS
    } else if (redisKeyPattern.contains("deviceid_md5")) {
      FeatureTypeEnum.USER
    } else {
      FeatureTypeEnum.ITEM
    }
  }
}
