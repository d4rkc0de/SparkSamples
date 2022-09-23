package org.d4rkc0de.schema

import org.apache.hadoop.io.DataOutputBuffer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.d4rkc0de.common.SparkFactory

object EmptyFile {
  def main(args: Array[String]) = {
    val spark = SparkFactory.getSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.emptyDataFrame.withColumn("test", lit("1"))
    spark.sparkContext.hadoopConfiguration.write(new DataOutputBuffer())
//    df.repartition(1).write.mode(SaveMode.Overwrite).option("header", "false")
//      .text("src/main/resources/output/files/test/trigger.txt")
  }
}
