package org.d4rkc0de.task_listener

import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListenerTaskStart}

trait TaskEventConsumer {
  def onTaskStart(taskStart: SparkListenerTaskStart)

  def onTaskEnd(taskEnd: SparkListenerTaskEnd)
}
