package window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, hash, max}
import org.d4rkc0de.common.SparkFactory

object WindowTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkFactory.getSparkSession()

    test2(spark)


    //    val test = df.orderBy(col("start").asc, col("end").asc)
    //      .withColumn("prevFin", lag("end", 1, 0).over(w))
    //    test.withColumn("groupId", when(col("start") > col("prevFin"), 1).otherwise(0))
    //      .withColumn("sum", sum('groupId) over w.rowsBetween(Window.unboundedPreceding, Window.currentRow))
    //    test.show()
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
