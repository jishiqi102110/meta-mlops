package com.meta.sys

import java.lang
import com.meta.conn.redis.JedisClusterName
import com.meta.entity.FeatureDTO.{FloatList, Int32List, Int64List, MAP_STRING_FLOAT, MAP_STRING_STRING, StringList}
import com.meta.entity.{FeatureDTO, FeatureTypeEnum}
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum
import com.meta.featuremeta.{RedisFeatureInfo, RedisFloatListMeta, RedisFloatMeta, RedisIntListMeta, RedisIntMeta, RedisLongListMeta, RedisMapFloatMeta, RedisMapStringMeta, RedisSeqListMeta, RedisStringListMeta, RedisStringMeta}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._ // scalastyle:ignore

/**
 * spark meta-utils 将hive数据解析为meta元数据并获取到特征序列化值,用户可以使用runSql 方式直接入库注册特征，这是离线特征入库的SDK
 * 用于离线特征平台
 *
 * @author weitaoliang
 * @version V1.0
 * */

object SparkMetaUtils {


  // method longer than 50 lines
  // scalastyle:off

  /**
   * 这里去获得每个特征对应原始信息构造 RedisFeatureInfo，以及从row原始信息转化成FeatureDTO bytes
   *
   * @Param [df] 去掉填充符的用户sql DataFrame
   * @Param [redisKeyPattern] 特征存储key egg:user_commonKey:{device_id}
   * @Param [jedisClusterName] 封装的特征存储集群
   * @Param [defaultValues] 特征默认值，这样避免默认值刷入redis,减少特征存储
   * @Param [dataSource] 数据源字符串，用来记录特征血缘
   */
  private def featureMetasAndGetFeatureMethods(df: DataFrame,
                                               redisKeyPattern: String,
                                               jedisClusterName: JedisClusterName,
                                               defaultValues: Map[String, Any],
                                               dataSource: String): Seq[(RedisFeatureInfo, Row => Array[Byte])] = {


    val _featureMetasAndGetFeatureMethods: Seq[(RedisFeatureInfo, Row => Array[Byte])] = {
      df.schema.fields.map {
        field =>
          val fieldName = field.name
          // step1:构造meta
          val meta = field.dataType match {
            // Int32类型
            case IntegerType =>
              // 根据默认值构造默认值对象
              val intDefaultVal = FeatureDTO.FieldValue.newBuilder()
                .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
                .setValue(FeatureDTO.Value.newBuilder
                  .setInt32Val(defaultValues.getOrElse(fieldName, 0).toString.toInt))
                .build()
              // 根据sql构造meta源信息
              new RedisIntMeta(jedisClusterName, redisKeyPattern, fieldName,
                dataSource, intDefaultVal, featureType(redisKeyPattern, fieldName))

            // double & float 采用float 主要是为了减少精度，减少存储
            case DoubleType | FloatType => // 统一采用float精度减少内存使用
              // 根据默认值构造默认值对象
              val floatDefaultVal = FeatureDTO.FieldValue.newBuilder()
                .setValueType(FeatureDTO.FieldValue.ValueType.FLOAT)
                .setValue(FeatureDTO.Value.newBuilder
                  .setFloatVal(defaultValues.getOrElse(fieldName, 0.0).toString.toFloat))
                .build()
              // 根据sql构造meta源信息
              new RedisFloatMeta(jedisClusterName, redisKeyPattern, fieldName,
                dataSource, floatDefaultVal, featureType(redisKeyPattern, fieldName))
            // String 类型特征，分为常规字符串特征及序列特征
            case StringType =>
              // 根据默认值构造默认值对象
              val StringDefaultVal = FeatureDTO.FieldValue.newBuilder()
                .setValueType(FeatureDTO.FieldValue.ValueType.STRING)
                .setValue(FeatureDTO.Value.newBuilder
                  .setStringVal(defaultValues.getOrElse(fieldName, 0).toString))
                .build()
              // 序列特征,序列类型的特征列名必须按照 time_seq_ 为前缀
              if (fieldName.startsWith("time_seq_")) {
                new RedisSeqListMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource,
                  StringDefaultVal, featureType(redisKeyPattern, fieldName))
              } else {
                // 常规字符串特征
                new RedisStringMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource,
                  true, StringDefaultVal, featureType(redisKeyPattern, fieldName))
              }
            // map类型,主要用来存储一些tag特征,暂时只支持 string类型或者float,根据经验其他类型特征基本使用率很低
            case map: MapType => {
              map.valueType match {
                // StringType IntegerType LongType 默认都转成 StringType
                case StringType | IntegerType | LongType => {
                  val mapDefault = defaultValues.getOrElse(fieldName, Map.empty[String, String])
                    .asInstanceOf[Map[String, String]]
                  val mapDefaultVal = FeatureDTO.FieldValue.newBuilder()
                    .setValueType(FeatureDTO.FieldValue.ValueType.MAP_STRING_STRING)
                    .setValue(FeatureDTO.Value.newBuilder()
                      .setMapStringStringVal(MAP_STRING_STRING.newBuilder().putAllVal(mapDefault)))
                    .build()
                  new RedisMapStringMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource,
                    mapDefaultVal, featureType(redisKeyPattern, fieldName))
                }
                //  FloatType DoubleType 默认都转成float
                case FloatType | DoubleType => {
                  val mapDefault = defaultValues.getOrElse(fieldName, Map.empty[String, Float])
                    .asInstanceOf[Map[String, java.lang.Float]].asJava
                  val mapDefaultVal = FeatureDTO.FieldValue.newBuilder()
                    .setValueType(FeatureDTO.FieldValue.ValueType.MAP_STRING_FLOAT)
                    .setValue(FeatureDTO.Value.newBuilder()
                      .setMapStringFloatVal(MAP_STRING_FLOAT.newBuilder().putAllVal(mapDefault)))
                    .build()
                  new RedisMapFloatMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource,
                    mapDefaultVal, featureType(redisKeyPattern, fieldName))
                }
              }
            }

            // array 类型特征，支持  IntegerType、StringType、LongType、FloatType、DoubleType
            case arr: ArrayType => {
              arr.elementType match {
                // Int32类型
                case IntegerType =>
                  val listDefault = defaultValues.getOrElse(fieldName, Array.empty[Integer]).asInstanceOf[Array[Integer]]
                  val listDefaultVal = FeatureDTO.FieldValue.newBuilder().setValueType(FeatureDTO.FieldValue.ValueType.INT32_LIST).setValue(FeatureDTO.Value.newBuilder.setInt32ListVal(Int32List.newBuilder().addAllVal(listDefault.toList.asJava).build())).build()
                  new RedisIntListMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource, listDefaultVal, featureType(redisKeyPattern, fieldName))
                // String类型
                case StringType => {
                  val listDefault = defaultValues.getOrElse(fieldName, Array.empty[String]).asInstanceOf[Array[String]]
                  val listDefaultVal = FeatureDTO.FieldValue.newBuilder().setValueType(FeatureDTO.FieldValue.ValueType.STRING_LIST).setValue(FeatureDTO.Value.newBuilder().setStringListVal(StringList.newBuilder().addAllVal(listDefault.toList.asJava).build())).build()
                  new RedisStringListMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource, listDefaultVal, featureType(redisKeyPattern, fieldName))
                }
                // Long类型
                case LongType => {
                  val listDefault = defaultValues.getOrElse(fieldName, Array.empty[lang.Long]).asInstanceOf[Array[lang.Long]]
                  val listDefaultVal = FeatureDTO.FieldValue.newBuilder().setValueType(FeatureDTO.FieldValue.ValueType.INT64_LIST).setValue(FeatureDTO.Value.newBuilder().setInt64ListVal(Int64List.newBuilder().addAllVal(listDefault.toList.asJava).build())).build()
                  new RedisLongListMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource, listDefaultVal, featureType(redisKeyPattern, fieldName))
                }
                // FloatType &  DoubleType 统一转成 FloatType
                case FloatType | DoubleType => {
                  val listDefault = defaultValues.getOrElse(fieldName, Array.empty[lang.Float]).asInstanceOf[Array[java.lang.Float]]
                  val listDefaultVal = FeatureDTO.FieldValue.newBuilder().setValueType(FeatureDTO.FieldValue.ValueType.FLOAT_LIST).setValue(FeatureDTO.Value.newBuilder.setFloatListVal(FloatList.newBuilder().addAllVal(listDefault.toList.asJava).build())).build()
                  new RedisFloatListMeta(jedisClusterName, redisKeyPattern, fieldName, dataSource, listDefaultVal, featureType(redisKeyPattern, fieldName))
                }
              }
            }
            case _ => throw new Exception("unsupported featureType in meta featurePlatForm")
          }
          // step2:获取 getFeature,将Row 转化成 FeatureDTO bytes
          val getFeature: Row => Array[Byte] = field.dataType match {
            // Int32类型
            case IntegerType => row => {
              val value = row.getAs[Integer](fieldName)
              val fieldValue = FeatureDTO.FieldValue.newBuilder()
                .setValueType(FeatureDTO.FieldValue.ValueType.INT32)
                .setValue(FeatureDTO.Value.newBuilder.setInt32Val(value))
                .build()
              // 调用meta序列化方法
              meta.serialize(fieldValue)
            }
            // String类型
            case StringType => row => {
              val value = row.getAs[String](fieldName)
              val fieldValue = FeatureDTO.FieldValue.newBuilder().
                setValueType(FeatureDTO.FieldValue.ValueType.STRING).
                setValue(FeatureDTO.Value.newBuilder.setStringVal(value)).
                build()
              meta.serialize(fieldValue)
            }
            // FloatType &  DoubleType 统一转成 FloatType
            case DoubleType => row => {
              val value = row.getAs[Double](fieldName)
              val fieldValue = FeatureDTO.FieldValue.newBuilder().
                setValueType(FeatureDTO.FieldValue.ValueType.FLOAT).
                setValue(FeatureDTO.Value.newBuilder.setFloatVal(value.toFloat)).
                build()
              meta.serialize(fieldValue)
            }
            // FloatType &  DoubleType 统一转成 FloatType
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

  /**
   * 通过解析dataFrame,得到特征抽象类及其对应的特征序列化bytes结果
   *
   * @Param [df] 去掉填充符的用户sql DataFrame
   * @Param [redisKeyPattern] 特征存储key egg:user_commonKey:{device_id}
   * @Param [jedisClusterName] 封装的特征存储集群
   * @Param [defaultValues] 特征默认值，这样避免默认值刷入redis,减少特征存储
   * @Param [dataSource] 数据源字符串，用来记录特征血缘
   */
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

  /** 根据 redisKeyPattern &  redisField 判断特征类型 */
  private def featureType(redisKeyPattern: String,
                          redisField: String): FeatureTypeEnum = {
    if (redisField != null && redisField.contains("{")) {
      FeatureTypeEnum.CROSS
    } else if (redisKeyPattern.contains("deviceid_md5")) { // 这里业务统一用 deviceid_md5标识用户，所以才用这个判断，可以自行更改
      FeatureTypeEnum.USER
    } else {
      FeatureTypeEnum.ITEM
    }
  }
}
