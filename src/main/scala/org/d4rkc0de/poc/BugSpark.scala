package org.d4rkc0de.poc

import org.d4rkc0de.common.SparkFactory

import java.sql.Date

object BugSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkFactory.getSparkSession()
    import spark.implicits._
    val columns = Seq("c1", "c2", "c3")
    val data = Seq((2, "aa", 3.0), (4, "bb", 2.2))
    val df = spark.sparkContext.parallelize(data).toDF(columns: _*)
    //    val df = Seq.empty[(String, Int)].toDF("k", "v")
    val data2 = Seq((1, new Date(2020,1,1), true), (4, new Date(2021,2,1), false))
    val df2 = spark.sparkContext.parallelize(data2).toDF(columns: _*)
    val res = df2.union(df)
    res.show()
    res.printSchema()
  }
}
