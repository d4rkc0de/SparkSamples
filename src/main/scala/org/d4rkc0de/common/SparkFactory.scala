package org.d4rkc0de.common

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.SparkSession
import org.joda.time.format.DateTimeFormat

object SparkFactory {

  def getSparkSession(): SparkSession = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkSamples")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sm = new StageEventManager
    sm.addEventConsumer(spark.sparkContext, "SEC1", new StageEventConsumerImpl)
    val rm = new TaskEventManager
    rm.addEventConsumer(spark.sparkContext, "RLEC1", new TaskEventConsumerImpl)
    //Register Task event listener
//    spark.sparkContext.addSparkListener(sm)
//    spark.sparkContext.addSparkListener(rm)

    spark
  }

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

  trait StageEventConsumer {
    def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

    def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit
  }

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

  trait TaskEventConsumer {
    def onTaskStart(taskStart: SparkListenerTaskStart)

    def onTaskEnd(taskEnd: SparkListenerTaskEnd)
  }

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
  }

  object DateUtils {
    def timeToStr(epochMillis: Long): String =
      DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").print(epochMillis)
  }

}
