package org.d4rkc0de.task_listener

import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted}

class StageEventConsumerImpl extends StageEventConsumer {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    println(
      s"------->Stage-${stageCompleted.stageInfo.stageId} completed" +
        s"\nstage name: ${stageCompleted.stageInfo.name} " +
        s"\nTasks count: ${stageCompleted.stageInfo.numTasks} " +
        s"\nexecutorRunTime=${stageCompleted.stageInfo.taskMetrics.executorRunTime} " +
        s"\nexecutorCPUTime=${stageCompleted.stageInfo.taskMetrics.executorCpuTime} " +
        s"\n<----------------")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println(s"------->Stage-${stageSubmitted.stageInfo.stageId} submitted" +
      s"\nstage name: ${stageSubmitted.stageInfo.name} " +
      s"\n<----------------")
  }
}
