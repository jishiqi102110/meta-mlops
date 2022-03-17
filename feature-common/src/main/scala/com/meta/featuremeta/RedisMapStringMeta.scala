package com.meta.featuremeta

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, SerializeTypeEnum}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum

/**
 * 对应的是 [[FeatureDTO.MAP_STRING_FLOAT]]
 *
 * @author weitaoliang
 * @version V1.0
 * */
class RedisMapStringMeta(jedisClusterName: JedisClusterName,
                          redisKeyPattern: String,
                          redisField: String,
                          dataSource: String,
                          defaultVal: FieldValue,
                          featureType: FeatureTypeEnum
                        ) extends RedisFeatureMeta[FeatureDTO.MAP_STRING_STRING](
  jedisClusterName, redisKeyPattern, redisField, dataSource, true, SerializeTypeEnum.PROTO,
  defaultVal, featureType) with Serializable
