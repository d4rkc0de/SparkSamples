package window

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, hash, lag, lit, max, monotonically_increasing_id, rank, row_number, when}
import org.d4rkc0de.common.SparkFactory

object WindowTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkFactory.getSparkSession()
    val t1 = System.nanoTime
    increasingIdByPartition_2(spark)
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


}
