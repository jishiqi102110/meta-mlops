package com.meta.streaming.spark.kafka

import scala.collection.mutable

/**
 * ctr 统计中用来统计点击和曝光次数方法
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class ShowClickAggregator extends Serializable {
  private[kafka] val condShowMap = new mutable.HashMap[(String, String), Int]
  private[kafka] val condClickMap = new mutable.HashMap[(String, String), Int]
  private[kafka] var totalShow = 0
  private[kafka] var totalClick = 0

  // add 操作
  def add(showClickInfo: (Int, Set[(String, String)])): this.type = {
    val (clicked, cond) = showClickInfo
    clicked match {
      case 0 =>
        // 更新曝光数
        totalShow += 1
        for (x <- cond) {
          if (condShowMap.contains(x)) {
            condShowMap(x) = condShowMap(x) + 1
          } else {
            condShowMap += x -> 1
          }
        }
      case 1 =>
        // 更新点击数
        totalClick += 1
        for (x <- cond) {
          if (condClickMap.contains(x)) {
            condClickMap(x) = condClickMap(x) + 1
          } else {
            condClickMap += x -> 1
          }
        }
    }
    this
  }

  // merge操作
  def merge(other: ShowClickAggregator): this.type = {
    totalShow += other.totalShow
    totalClick += other.totalClick
    for ((x, num) <- other.condShowMap) {
      if (condShowMap.contains(x)) {
        condShowMap(x) = condShowMap(x) + num
      } else {
        condShowMap += x -> num
      }
    }
    for ((x, num) <- other.condClickMap) {
      if (condClickMap.contains(x)) {
        condClickMap(x) = condClickMap(x) + num
      } else {
        condClickMap += x -> num
      }
    }
    this
  }
}
