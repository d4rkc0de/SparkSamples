package org.d4rkc0de.common

import org.apache.spark.sql.SparkSession
import org.d4rkc0de.task_listener.{StageEventConsumerImpl, StageEventManager, TaskEventConsumerImpl, TaskEventManager}

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
}
