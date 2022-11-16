package com.meta.utils

/**
 * feature-common 的常量类
 *
 * @author weitaoliang
 * @version V1.0
 */
object CommonConstants {


  ///////////////////////////////////////////////////////////////////////////
  // feature-common
  ///////////////////////////////////////////////////////////////////////////

  final val DEFAULT_EXECUTOR_NUMBERS = 10

  final val REDIS_WRITE_PERMIT = 5000

  final val TEST_REDIS_TTL = 3600


  ///////////////////////////////////////////////////////////////////////////
  // feature-flow
  ///////////////////////////////////////////////////////////////////////////

  // netWork type
  final val NET_WORK_TYPE_1 = "13-1-1"
  final val NET_WORK_TYPE_2 = "13-1-2"
  final val NET_WORK_TYPE_3 = "13-1-3"
  final val NET_WORK_TYPE_4 = "13-1-4"
  final val NET_WORK_TYPE_5 = "13-1-5"
  final val NET_WORK_TYPE_6 = "13-1-6"
  final val NET_WORK_TYPE_7 = "13-1-7"
  final val NET_WORK_TYPE_8 = "13-1-8"

  // Hbase
  final val HBASE_PRE_PARTITION_NUM = 1000
  final val HBASE_BUFFER_TIME_OUT = 10 * 60
  final val HBASE_REQUEST_ID_NAME = "requestID"
  final val HBASE_REQUEST_TIMESTAMP_NAME = "requestTimeStamp"


  ///////////////////////////////////////////////////////////////////////////
  // feature-online
  ///////////////////////////////////////////////////////////////////////////

  final val DEFAULT_BATCH_DURATION = 0

  final val DEFAULT_DELAY_DURATION = 10

  final val DEFAULT_CTR_REDIS_TTL = 90 * 24 * 60 * 60

  final val DEFAULT_CTR_ALPHA = 0.9999f

  final val DEFAULT_SHOW = 2000f

  final val DEFAULT_CLICK = 10f

  final val DEFAULT_CTR = 0.0f

}
