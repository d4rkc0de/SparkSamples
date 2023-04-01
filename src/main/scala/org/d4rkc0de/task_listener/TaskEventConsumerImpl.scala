package org.d4rkc0de.task_listener

import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListenerTaskStart}
import org.joda.time.format.DateTimeFormat

class TaskEventConsumerImpl extends TaskEventConsumer {
  def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {

    println(
      s"------->Task-${taskStart.taskInfo.index}  of Stage-${taskStart.stageId} Started-------->" +
        s"\nId: ${taskStart.taskInfo.taskId} " +
        s"\nExecutor Id: ${taskStart.taskInfo.executorId} " +
        s"\nHost: ${taskStart.taskInfo.host} " +
        s"\nLaunchTime: ${DateUtils.timeToStr(taskStart.taskInfo.launchTime)} " +
        s"\n<----------------")

  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println(
      s"------->Task-${taskEnd.taskInfo.index}  of Stage-${taskEnd.stageId} Completed-------->" +
        s"\nId: ${taskEnd.taskInfo.taskId} " +
        s"\nTaskType: ${taskEnd.taskType} " +
        s"\nExecutor Id: ${taskEnd.taskInfo.executorId} " +
        s"\nHost: ${taskEnd.taskInfo.host} " +
        s"\nFinish Time: ${DateUtils.timeToStr(taskEnd.taskInfo.finishTime)} " +
        s"\nReason: ${taskEnd.reason} " +
        s"\nRecords Written=${taskEnd.taskMetrics.outputMetrics.recordsWritten} " +
        s"\nRecords Read=${taskEnd.taskMetrics.inputMetrics.recordsRead} " +
        s"\nExecutor RunTime=${taskEnd.taskMetrics.executorRunTime} " +
        s"\nExecutor Cpu Time=${taskEnd.taskMetrics.executorCpuTime} " +
        s"\nPeakExecutionMemory: ${taskEnd.taskMetrics.peakExecutionMemory} " +
        s"\n<----------------")
  }

  object DateUtils {
    def timeToStr(epochMillis: Long): String =
      DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").print(epochMillis)
  }
}
