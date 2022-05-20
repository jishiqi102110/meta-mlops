package com.meta.data.conf

import com.meta.featuremeta.RedisFeatureInfo

/**
 * description 
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class RedisFeatureMetaWrapper(val featureName: String,
                              val featureInfo: RedisFeatureInfo,
                              val isCache: Boolean) extends Serializable