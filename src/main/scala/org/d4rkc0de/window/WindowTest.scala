package org.d4rkc0de.window

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, first, hash, lag, lit, max, monotonically_increasing_id, rank, row_number, to_date, to_timestamp, when}
import org.d4rkc0de.common.SparkFactory

object WindowTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkFactory.getSparkSession()
    val t1 = System.nanoTime
    firstValueByPartition(spark)
    val duration = (System.nanoTime - t1) / 1e9d
    println("Duration === " + duration)

    //    val test = df.orderBy(col("start").asc, col("end").asc)
    //      .withColumn("prevFin", lag("end", 1, 0).over(w))
    //    test.withColumn("groupId", when(col("start") > col("prevFin"), 1).otherwise(0))
    //      .withColumn("sum", sum('groupId) over w.rowsBetween(Window.unboundedPreceding, Window.currentRow))
    //    test.show()
  }

  def increasingIdByPartition_2(spark: SparkSession) = {
    val columnName = "Industry_code_NZSIOC"
    val df = spark.read.format("csv")
      .option("header", "true").option("delimiter", ",").option("quote", "\"").option("escape", "\"")
      .load("src/main/resources/input/files/annual-enterprise.csv")
    val dfGroupBy = df.select(col(columnName).as(columnName + "_2")).distinct().withColumn("idx", monotonically_increasing_id())
    df.join(dfGroupBy, col(columnName) === col(columnName + "_2"), "left").drop(columnName + "_2").orderBy("idx").show(false)
  }

  def increasingIdByPartition(spark: SparkSession) = {
    val columnName = "Industry_code_NZSIOC"
    val df = spark.read.format("csv")
      .option("header", "true").option("delimiter", ",").option("quote", "\"").option("escape", "\"")
      .load("src/main/resources/input/files/annual-enterprise.csv")
    val w = Window.partitionBy(columnName).orderBy(columnName)
    val w2 = Window.orderBy(columnName)
    df.withColumn("prev", lag(columnName, 1, null).over(w))
      .withColumn("id", when(col(columnName).isNull || col(columnName) === col("prev"), 0).otherwise(1))
      .withColumn("id", functions.sum("id") over w2.rowsBetween(Window.unboundedPreceding, Window.currentRow)).show()
  }

  def testMaxPartition(spark: SparkSession) = {
    import spark.implicits._
    val columns = Seq("partition", "start", "end")
    val data = Seq(
      (1, 2, 5),
      (1, 4, 7),
      (1, 2, 3),
      (1, 4, 114),
      (2, 3, 4),
      (2, 12, 25),
      (2, 10, 14),
      (2, 77, 99),
      (3, 12, 20)
    )
    val df = spark.sparkContext.parallelize(data).toDF(columns: _*)
    val w = Window.partitionBy("partition")
    df.withColumn("maxByPartition", max("end").over(w)).show()
  }

  def test1(spark: SparkSession): Unit = {
    import spark.implicits._
    val columns = Seq("start", "end")
    val data = Seq((2, 5),
      (4, 7),
      (2, 3),
      (4, 6),
      (3, 4),
      (12, 25),
      (10, 14),
      (77, 99),
      (12, 20)
    )
    val df = spark.sparkContext.parallelize(data).toDF(columns: _*)
    val w = Window.orderBy(col("start").asc, col("end").desc)

    val sortedDf = df
      .withColumn("max", max('fin) over w.rowsBetween(Window.unboundedPreceding, Window.currentRow))
      .withColumn("groupId", hash(col("max")))
    sortedDf.show()
  }

  def test2(spark: SparkSession): Unit = {
    import spark.implicits._
    val columns = Seq("start", "end")
    val data = Seq(
      (1, 5),
      (2, 7),
      (6, 9),
      (6, 9),
      (7, 12),
      (9, 14),
      (13, 15),
      (20, 30),
      (27, 29)
    )
    val df = spark.sparkContext.parallelize(data).toDF(columns: _*)
    val w = Window.orderBy(col("start").asc, col("end").desc)
    df.withColumn("rank", dense_rank().over(w)).show()

    val data2 = Seq(
      (1, 5),
      (6, 9),
      (13, 15),
      (20, 30),
    )
    spark.sparkContext.parallelize(data2).toDF(columns: _*).show()
  }

  def datesIntersection(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq(
      ("2019-07-01 10:01:19.000", "2019-07-01 10:11:00.000"),
      ("2019-07-01 10:10:05.000", "2019-07-01 10:40:00.000"),
      ("2019-07-01 10:35:00.000", "2019-07-01 12:30:00.000"),
      ("2019-07-01 15:20:00.000", "2019-07-01 15:50:00.000"),
      ("2019-07-01 16:10:00.000", "2019-07-01 16:35:00.000"),
      ("2019-07-01 16:30:00.000", "2019-07-01 17:00:00.000"),
    ).toDF("start", "end")

    // A window to sort date by start ascending then end ascending, to get the end of the previous row to check if there's an intersection
    val w = Window.orderBy("start", "end")
    // transform column from string type to timestamp type
    df.select(to_timestamp(col("start")).as("start"), to_timestamp(col("end")).as("end"))
      // prev_end column contains the value of the end column of the previous row
      .withColumn("prev_end", lag("end", 1, null).over(w))
      // create column intersection with value 0 if there's intersection and 1 otherwhise
      .withColumn("intersection", when(col("prev_end").isNull.or(col("prev_end").geq(col("start")).and(col("prev_end").leq(col("end")))), 0).otherwise(1))
      // The key element to this solution: prefix sum over the window to make sure we have the right values of each group
      .withColumn("group", functions.sum("intersection").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .drop("prev_end", "intersection")
      .show(false)
  }

  def firstValueByPartition(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      ("AAA", "a", 1),
      ("AAA", "b", 2),
      ("BBB", "c", 7),
      ("BBB", "c", 4),
      ("AAA", "c", 3),
      ("BBB", "c", 5),
      ("CCC", "x", 1),
    ).toDF("id", "value", "time")

    val window = Window.partitionBy("id").orderBy("time")
    val newDf = df.withColumn("value", first("value").over(window))
      .withColumn("time", first("time").over(window))
    newDf.show(false)
  }

}
