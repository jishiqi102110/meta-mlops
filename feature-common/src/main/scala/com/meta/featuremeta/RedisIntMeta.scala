package com.meta.featuremeta

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, SerializeTypeEnum}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum


/**
 * INT类型特征抽象 对应的是 [[FeatureDTO.INT32]]
 *
 * @author weitaoliang
 * @version V1.0
 * */

class RedisIntMeta(
                    jedisClusterName: JedisClusterName,
                    redisKeyPattern: String,
                    redisField: String,
                    dataSource: String,
                    defaultVal: FieldValue,
                    featureType: FeatureTypeEnum
                  ) extends RedisFeatureMeta[FeatureDTO.INT32](
  jedisClusterName, redisKeyPattern, redisField, dataSource, false,
  SerializeTypeEnum.PROTO, defaultVal, featureType) with Serializable {

  override def maxAndMin(t: Any): (Double, Double) = (t.asInstanceOf[Int], t.asInstanceOf[Int])
}
