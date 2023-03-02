package org.d4rkc0de.ui

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.d4rkc0de.common.SparkFactory
import org.d4rkc0de.ui.SparkUI.spark

object SparkUI extends App {
  val spark = SparkFactory.getSparkSession()

  test(spark)

  def test2(spark: SparkSession) = {
    val df = spark.read.option("header", "true").option("delimiter", ",")
      .csv("src/main/resources/input/files/bigfile.csv")
      .withColumn("count", col("count").cast(IntegerType))
      //    .withColumn("Value", regexp_replace(col("Value"), ",", ".").cast(DoubleType))
      .repartition(16, col("Year"))
    val t1 = System.nanoTime

    val res = df.groupBy("Year").sum("count")
    res.count()
    println(df.rdd.getNumPartitions)
    println(res.rdd.getNumPartitions)
    val duration = (System.nanoTime - t1) / 1e9d
    println("Duration === " + duration)
    Thread.sleep(1000000)
  }

  def test(spark: SparkSession) = {
    val path = "target/test-data/test.parquet"
    val df = spark.read.option("header", "true").option("delimiter", ",")
      .csv("src/main/resources/input/files/bigfile.csv")
      .withColumn("count", col("count").cast(IntegerType))
      //    .withColumn("Value", regexp_replace(col("Value"), ",", ".").cast(DoubleType))
      .repartition(16, col("Year"))

    df.write.mode(SaveMode.Overwrite).parquet(path)

    val parquetFileDF = spark.read.parquet(path)
    parquetFileDF.createOrReplaceTempView("Test")
    parquetFileDF.show(false)
  }
}
