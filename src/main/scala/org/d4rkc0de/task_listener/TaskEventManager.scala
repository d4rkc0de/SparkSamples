package org.d4rkc0de.task_listener

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart}

class TaskEventManager extends SparkListener {
  var consumerMap: scala.collection.mutable.Map[String, TaskEventConsumer] = scala.collection.mutable.Map[String, TaskEventConsumer]()

  def addEventConsumer(SparkContext: SparkContext, id: String, consumer: TaskEventConsumer) {
    consumerMap += (id -> consumer)
  }

  def removeEventConsumer(id: String) {
    consumerMap -= id
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    for ((_, v) <- consumerMap) {
      if (v != null) {
        v.onTaskStart(taskStart)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    for ((_, v) <- consumerMap) {
      if (v != null) {
        v.onTaskEnd(taskEnd)
      }
    }
  }
}
