package com.meta.spark.monitor

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.mapreduce.jobhistory.TaskFailed
import org.apache.spark.{ExceptionFailure, ExecutorLostFailure, FetchFailed, TaskCommitDenied, TaskFailedReason, TaskKilled}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

/**
 * 扩展sparkListener 用来监听spark异步任务
 *
 * spark异步接口任务只有大于task级别的失败才会停止，task级别失败不会停止任务，会导致线上任务一直失败而不知道，所以需要开发一个task级别的监听器，
 * task失败时进行监控告警 使用的时候只需要添加此类即可
 * spark.sparkContext.addSparkListener(new SparkApplistener(spark))
 *
 * @author: weitaoliang
 * @version v1.0
 * */
class SparkApplistener(spark: SparkSession) extends SparkListener with Logging with Serializable {
  // 这里只监控task，是因为只有在task
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    if (taskInfo != null && taskEnd.stageAttemptId != -1) {
      val errorMessage: Option[String] = {
        taskEnd.reason match {
          case killed: TaskKilled =>
            Some(killed.toErrorString)
          case exceptionFailure: ExceptionFailure =>
            Some(exceptionFailure.toErrorString)
          case taskFailed: TaskFailedReason =>
            Some(taskFailed.toErrorString)
          case executorFailure: ExecutorLostFailure =>
            Some(executorFailure.toErrorString)
          case fetchFailed: FetchFailed =>
            Some(fetchFailed.toErrorString)
          case taskCommitDenied: TaskCommitDenied =>
            Some(taskCommitDenied.toErrorString)
          case _ =>
            Some(taskEnd.reason.toString)
        }
      }
      // 这里整理task错误信息
      val taskID = taskInfo.taskId
      val stageID = taskEnd.stageId
      val appID = spark.sparkContext.applicationId
      val appName = spark.sparkContext.appName
      val user = spark.sparkContext.sparkUser
      val message = new JSONObject()
      message.put("taskID", taskID)
      message.put("stageID", stageID)
      message.put("appID", appID)
      message.put("appName", appName)
      message.put("user", user)
      message.put("taskError", errorMessage.get)

      //这里可做一个定时器，然后出现任务失败时，触发自定义条件告警
    }
  }


}
