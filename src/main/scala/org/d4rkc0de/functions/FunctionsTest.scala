package org.d4rkc0de.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, explode}
import org.d4rkc0de.common.SparkFactory

object FunctionsTest extends App {
  val spark = SparkFactory.getSparkSession()
  testExplode(spark)

  def testExplode(spark: SparkSession) = {
    import spark.implicits._
    val inputSmall = Seq(
      ("A", 0.3, "B", 0.25),
      ("A", 0.3, "g", 0.4),
      ("d", 0.0, "f", 0.1),
      ("d", 0.0, "d", 0.7),
      ("A", 0.3, "d", 0.7),
      ("d", 0.0, "g", 0.4),
      ("c", 0.2, "B", 0.25)).toDF("column1", "transformedCol1", "column2", "transformedCol2")
    val result = inputSmall.withColumn("combined", explode(array($"transformedCol1", $"transformedCol2")))
    result.show(false)
  }
}
