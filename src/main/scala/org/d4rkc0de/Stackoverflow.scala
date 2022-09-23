package org.d4rkc0de

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.d4rkc0de.common.SparkFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object Stackoverflow {
  def main(args: Array[String]): Unit = {
    val spark = SparkFactory.getSparkSession()
    q_73142900(spark)
  }

  def q_73038844(spark: SparkSession) = {
    import spark.implicits._
    //    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val columns = Seq("start", "end")
    val data = Seq((2, 5), (4, 7))
    val df = spark.sparkContext.parallelize(data).toDF(columns: _*)
    val results = df.withColumn("MONTH",
      explode(
        sequence(
          lit("2016-02-01").cast(DataTypes.TimestampType),
          lit("2016-05-31").cast(DataTypes.TimestampType),
          expr("INTERVAL 1 month")
        )
      )
    )
    results.show()
  }

  def q_73142900(spark: SparkSession) = {
    import spark.implicits._
    //    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val columns = Seq("Date", "User")
    val data = Seq(("2022-05-01", "A"), ("2022-05-02", "A"), ("2022-05-03", "A"), ("2022-05-05", "A"), ("2022-05-06", "A"),
      ("2022-05-01", "B"), ("2022-05-03", "B"), ("2022-05-04", "B"), ("2022-05-05", "B"), ("2022-05-06", "B"),
      ("2022-05-01", "C"), ("2022-05-02", "C"), ("2022-05-04", "C"), ("2022-05-05", "C"), ("2022-05-06", "C")
    )
    val df = spark.sparkContext.parallelize(data).toDF(columns: _*)
    val w = Window.partitionBy("User").orderBy("Date")
    val w2 = Window.partitionBy("User", "partition").orderBy("Date")
    val results = df.withColumn("prev_date", lag("Date", 1, null).over(w))
      .withColumn("date_diff", datediff(col("date"),col("prev_date")))
      .withColumn("tmp", when(col("prev_date").isNull.or(col("date_diff").equalTo(1)), 0).otherwise(1))
      .withColumn("partition", sum("tmp").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .withColumn("Rank", dense_rank().over(w2))
      .select("Date", "User", "Rank")
    results.show()
  }

}
