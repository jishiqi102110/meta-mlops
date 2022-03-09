package com.meta.featuremeta

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, SerializeTypeEnum}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum


/**
 * 对应的是 [[FeatureDTO.STRING]]
 *
 * @author weitaoliang
 * @version V1.0
 * */
class RedisStringMeta(jedisClusterName: JedisClusterName,
                      redisKeyPattern: String,
                      redisField: String,
                      dataSource: String,
                      isCompress: Boolean,
                      defaultVal: FieldValue,
                      featureType: FeatureTypeEnum) extends RedisFeatureMeta[FeatureDTO.STRING](
  jedisClusterName, redisKeyPattern, redisField, dataSource, isCompress, SerializeTypeEnum.PROTO,
  defaultVal, featureType) with Serializable
