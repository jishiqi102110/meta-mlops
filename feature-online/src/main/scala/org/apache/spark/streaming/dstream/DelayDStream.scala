package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Interval, Seconds, Time}

import scala.reflect.ClassTag

/**
 * 延迟执行DStreamm默认延迟执行10s
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class DelayDStream[T: ClassTag](parent: DStream[T]) extends DStream[T](parent.ssc) {
  override def slideDuration: Duration = parent.slideDuration

  private final val DEFAULT_DELAY_SECONDS = 10

  // 延迟执行时间
  private var _delayDuration: Duration = {
    if (parent.slideDuration < Seconds(DEFAULT_DELAY_SECONDS)) {
      parent.slideDuration * math.ceil(Seconds(DEFAULT_DELAY_SECONDS) / parent.slideDuration).toInt
    } else {
      parent.slideDuration
    }
  }

  def setDelayDuration(_delayDuration: Duration): DStream[T] = {
    this._delayDuration = parent.slideDuration * math.ceil(_delayDuration / parent.slideDuration).toInt
    this
  }

  def delayDuration: Duration = _delayDuration

  override def dependencies: List[DStream[_]] = List(parent)

  override def parentRememberDuration: Duration = rememberDuration + delayDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    val currentWindow = new Interval(validTime - delayDuration, validTime - delayDuration)
    val windowRDDs = parent.slice(currentWindow)
    Some(ssc.sc.union(windowRDDs))
  }
}
