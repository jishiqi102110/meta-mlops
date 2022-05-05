package com.meta.featuremeta

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, SerializeTypeEnum}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum

/**
 * 序列特征格式，存储的格式还是为Array[String]，但是格式固定，为针对当前业务专门为序列特征抽象的格式，
 * 格式为timeStamp1:x1    timeStamp2:y1  一个时间戳对应一个序列
 * 对应的是 [[FeatureDTO.SeqList]]
 *
 * @author weitaoliang
 * @version V1.0
 * */

class RedisSeqList(jedisClusterName: JedisClusterName,
                   redisKeyPattern: String,
                   redisField: String,
                   dataSource: String,
                   defaultVal: FieldValue,
                   featureType: FeatureTypeEnum) extends RedisFeatureMeta[FeatureDTO.SeqList](
  jedisClusterName, redisKeyPattern, redisField, dataSource, true, SerializeTypeEnum.PROTO,
  defaultVal, featureType) with Serializable
