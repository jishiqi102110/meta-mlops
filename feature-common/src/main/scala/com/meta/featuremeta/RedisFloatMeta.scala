package com.meta.featuremeta

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, SerializeTypeEnum}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum


/**
 * [[FeatureDTO.FLOAT]]
 *
 * @author weitaoliang
 * @version V1.0
 * */
class RedisFloatMeta(jedisClusterName: JedisClusterName,
                     redisKeyPattern: String,
                     redisField: String,
                     dataSource: String,
                     defaultVal: FieldValue,
                     featureType: FeatureTypeEnum) extends RedisFeatureMeta[FeatureDTO.FLOAT](
  jedisClusterName, redisKeyPattern, redisField, dataSource, false,
  SerializeTypeEnum.PROTO, defaultVal, featureType) with Serializable {
  override def maxAndMin(t: Any): (Double, Double) = (t.asInstanceOf[Float], t.asInstanceOf[Float])
}
