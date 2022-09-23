package org.d4rkc0de.common

import org.apache.spark.sql.SparkSession

object SparkFactory {

  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkSamples")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}
