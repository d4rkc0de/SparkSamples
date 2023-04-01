package org.d4rkc0de.task_listener

import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted}

trait StageEventConsumer {
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit
}