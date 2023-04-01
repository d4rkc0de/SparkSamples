package org.d4rkc0de.task_listener

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerStageSubmitted}
class StageEventManager extends SparkListener {
  private val consumerMap: scala.collection.mutable.Map[String, StageEventConsumer] = scala.collection.mutable.Map[String, StageEventConsumer]()

  def addEventConsumer(SparkContext: SparkContext, id: String, consumer: StageEventConsumer) {
    consumerMap += (id -> consumer)
  }

  def removeEventConsumer(id: String) {
    consumerMap -= id
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    consumerMap.foreach { case (_, v) =>
      if (v != null) {
        v.onStageSubmitted(stageSubmitted)
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    consumerMap.foreach { case (_, v) =>
      if (v != null) {
        v.onStageCompleted(stageCompleted)
      }
    }
  }
}
