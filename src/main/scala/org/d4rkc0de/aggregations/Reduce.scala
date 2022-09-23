package org.d4rkc0de.aggregations

import org.d4rkc0de.common.SparkFactory

object Reduce {
  def main(args: Array[String]) = {

    val spark = SparkFactory.getSparkSession()
    spark.sparkContext.setLogLevel("WARN")


    var df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load("src/main/resources/input/files/zipcodes.csv")

//    df.groupByKey()
  }
}
