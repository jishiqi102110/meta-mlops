package com.meta.featuremeta

import com.meta.conn.redis.JedisClusterName
import com.meta.entity.{FeatureDTO, SerializeTypeEnum}
import com.meta.entity.FeatureDTO.FieldValue
import com.meta.entity.FeatureTypeEnum.FeatureTypeEnum

/**
 * 对应的是 [[FeatureDTO.SeqList]],默认采用proto序列化方式及snappy压缩
 * 序列特征格式，存储的格式还是为Array[String]，但是格式固定，为针对当前业务专门为序列特征抽象的格式，
 * 格式为timeStamp1:x1    timeStamp2:y1  一个时间戳对应一个序列
 *
 * jedisClusterName 存储的集群类
 * redisKeyPattern 特征存储key，一般形式为 常量+{deviceid},{}中为填充符
 * redisField 特征存储field,如果feild是空，则特征为kv结构，如果不为空则为hash结构，另外如果field中也包含{},里面也包含填充符,则该特征为交叉特征
 * dataSource 特征数据源，一般填存储离线(twd、hive)、实时(tdbank、kafka)地址
 * defaultVal 特征默认值，用于注册时定义，可以减少默认值入库
 * featureType 特征类型，暂时分为 user、item、cross、scene 用户、物品、交叉、场景特征
 * @author weitaoliang
 * @version V1.0
 * */

class RedisSeqListMeta(jedisClusterName: JedisClusterName,
                   redisKeyPattern: String,
                   redisField: String,
                   dataSource: String,
                   defaultVal: FieldValue,
                   featureType: FeatureTypeEnum) extends RedisFeatureMeta[FeatureDTO.SeqList](
  jedisClusterName, redisKeyPattern, redisField, dataSource, true, SerializeTypeEnum.PROTO,
  defaultVal, featureType) with Serializable
