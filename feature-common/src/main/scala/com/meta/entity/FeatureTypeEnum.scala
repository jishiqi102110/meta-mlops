package com.meta.entity

object FeatureTypeEnum extends Enumeration {
  type FeatureTypeEnum = Value

  // 上下文特征
  val SCENE: Value = Value("scene")
  // 用户特征
  val USER: Value = Value("user")
  // item特征
  val ITEM: Value = Value("item")
  // cross特征
  val CROSS: Value = Value("cross")
}
