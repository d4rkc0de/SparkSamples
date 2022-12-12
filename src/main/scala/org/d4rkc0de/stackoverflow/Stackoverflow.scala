package org.d4rkc0de.stackoverflow

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}
import org.d4rkc0de.common.SparkFactory

import scala.util.Random


object Stackoverflow extends App {
  val spark = SparkFactory.getSparkSession()
  q_74776290(spark)

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
      .withColumn("date_diff", datediff(col("date"), col("prev_date")))
      .withColumn("tmp", when(col("prev_date").isNull.or(col("date_diff").equalTo(1)), 0).otherwise(1))
      .withColumn("partition", sum("tmp").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .withColumn("Rank", dense_rank().over(w2))
      .select("Date", "User", "Rank")
    results.show()
  }

  def q_74625054(spark: SparkSession) = {
    import org.apache.spark.sql.{DataFrameReader, SaveMode, SparkSession}
    import org.apache.hadoop.fs.Path
    val path = "dbfs:/mnt/qse-ass-blob/sales/test/"
    val hdfs = new Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration)
    hdfs.listStatus(new Path(path)).foreach(file => if (file.isDirectory)
      spark.read.csv(path + file.getPath.getName).coalesce(1)
        .write.mode(SaveMode.Overwrite).csv(path + file.getPath.getName)
    )
  }

  def q_74753985(spark: SparkSession) = {
    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row("first", 2.0),
        Row("test", 1.5),
        Row("choose", 8.0)
      )
    )

    val schema: StructType = new StructType()
      .add(StructField("id", StringType, true))
      .add(StructField("val1", DoubleType, true))

    val dfWithSchema = spark.createDataFrame(rdd, schema)
    dfWithSchema.withColumn("id", concat(col("id"), lit(Random.nextString(10)))).show()

  }

  def q_74762704(spark: SparkSession) = {
    val df1 = spark.range(1, 10000).repartition(50)
    val df2 = spark.range(1, 100000).repartition(3)
    val df3 = df1.join(df2, Seq("id"), "inner")
    println(df3.rdd.getNumPartitions)
    df3.explain()
  }


  def q_74774644(spark: SparkSession) = {
    val arrayStructData = Seq(
      Row(List(Row("123", "123", "123"), Row("abc", "def", "ghi"), Row("jkl", "mno", "pqr"), Row("0", "1", "2"))),
      Row(List(Row("456", "456", "456"), Row("qsd", "fgh", "hjk"), Row("aze", "rty", "uio"), Row("4", "5", "6"), Row("7", "8", "9")))
    )

    val arrayStructSchema = new StructType()
      .add("targeting_values", ArrayType(new StructType()
        .add("_1", StringType)
        .add("_2", StringType)
        .add("_3", StringType)))

    val df = spark.createDataFrame(spark.sparkContext
      .parallelize(arrayStructData), arrayStructSchema)

    df.show(false)

    df.withColumn("id2", monotonically_increasing_id())
      .select(col("id2"), posexplode(col("targeting_values")))
      .withColumn("id", concat(lit("value"), col("pos") + 1))
      .groupBy("id2").pivot("id").agg(first("col")).drop("id2")
      .show(false)
  }

  def q_74776290(spark: SparkSession) = {
    import spark.implicits._
    val data = Seq(
      ("weight=100,height=70", Seq("weight", "height")),
      ("weight=92,skinCol=white", Seq("weight", "skinCol")),
      ("hairCol=gray,skinCol=white", Seq("hairCol", "skinCol"))
    )

    val df = spark.sparkContext.parallelize(data).toDF("info", "chars")
      .withColumn("id", monotonically_increasing_id() + 1)

    val pivotDf = df
      .withColumn("tmp", split(col("info"), ","))
      .withColumn("tmp", explode(col("tmp")))
      .withColumn("val1", split(col("tmp"), "=")(0))
      .withColumn("val2", split(col("tmp"), "=")(1)).select("id", "val1", "val2")
      .groupBy("id").pivot("val1").agg(first(col("val2")))

    df.join(pivotDf, Seq("id"), "left").drop("id").show(false)

    df.withColumn("tmp", explode(split(col("info"), ",")))
      .withColumn("values", split(col("tmp"), "=")(0)).select("values").distinct().show()

  }


  def q_74777265(spark: SparkSession) = {
    import spark.implicits._
    val data = Seq(
      ("234", Seq("43", "54")),
      ("65", Seq("95")),
      ("all", Seq()),
      ("76", Seq("84", "67"))
    )

    val df = spark.sparkContext.parallelize(data).toDF("apples_logic_string", "apples_set")
    val allDf = df.select(explode(col("apples_set")).as("apples_set")).agg(collect_set("apples_set").as("all_apples_set"))
      .withColumn("apples_logic_string", lit("all"))
    df.join(broadcast(allDf), Seq("apples_logic_string"), "left")
      .withColumn("apples_set", when(col("apples_logic_string").equalTo("all"), col("all_apples_set")).otherwise(col("apples_set")))
      .drop("all_apples_set").show(false)
  }


}
