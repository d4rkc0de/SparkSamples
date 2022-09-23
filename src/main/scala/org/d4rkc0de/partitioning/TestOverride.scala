package org.d4rkc0de.partitioning

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.d4rkc0de.common.SparkFactory

object TestOverride {
  def main(args: Array[String]): Unit = {

    val spark = SparkFactory.getSparkSession()
    //    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
//    spark.sparkContext.setLogLevel("WARN")

    var df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load("src/main/resources/input/files/partitions")
//          .filter(col("dhi").equalTo("2022"))

    df.explain("formatted")

    df.write
      .partitionBy("dhi")
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/output/partitions")
  }
}
