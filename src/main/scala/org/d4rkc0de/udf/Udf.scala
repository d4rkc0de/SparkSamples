package org.d4rkc0de.udf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.d4rkc0de.common.SparkFactory

object Udf extends App {
  val spark = SparkFactory.getSparkSession()

  import spark.implicits._

  val columns = Seq("c1", "c2")
  val data = Seq((4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7), (4, 7))
  val df = spark.sparkContext.parallelize(data).toDF(columns: _*)
  df.rdd.getNumPartitions
  seq_udf(df)

  def seq_udf(df: DataFrame) = {
    var n = -1;
    val createSeq = () => {
      n = n + 1
      (1 + Math.sqrt(1 + (8 * n)).toInt) / 2
    }

    //Using with DataFrame
    val createSeqUDF = udf(createSeq)
    df.withColumn("c3", createSeqUDF()).show(false)


  }
}
