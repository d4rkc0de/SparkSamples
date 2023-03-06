package org.d4rkc0de.stackoverflow

import org.apache.spark.sql.{DataFrame, Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json

import org.apache.spark.sql.types.{ArrayType, DataTypes, DoubleType, IntegerType, MapType, StringType, StructField, StructType}
import org.d4rkc0de.common.SparkFactory
import org.d4rkc0de.models.ZipCode

import scala.collection.Seq
import scala.util.Random
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml._
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier

object Stackoverflow extends App {
  val spark = SparkFactory.getSparkSession()
  q_75649269(spark)

  def q_75649269(spark: SparkSession) = {
    import spark.implicits._
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    val df1 = Seq(("abc", "2017-08-01")).toDF("id", "eventTime")
    val df2 = df1.withColumn("eventTime1", to_timestamp(col("eventTime"), "yyyy-MM-dd"))
    df2.show()
  }

  def q_75476216(spark: SparkSession) = {
    import spark.implicits._
    val df = spark.read.option("multiline", "true").json("src/main/resources/input/files/75476216.json")
    val schema = new StructType()
      .add("key", StringType, true)
      .add("value", MapType(StringType, IntegerType), true)

    val res = df.withColumn("student_data", from_json(col("student_data"), ArrayType(schema)))
      .select(col("id"), col("name"), explode(col("student_data")).as("student_data"))
      .select("id", "name", "student_data.*")
      .select(col("id"), col("name"), col("key"), map_values(col("value")).getItem(0).as("value"))

    res.groupBy("id", "name").pivot("key").agg(first(col("value"))).show(false)
  }

  def q_75371783(spark: SparkSession) = {
    import spark.implicits._
    val dfIn = spark.createDataFrame(Seq(
      ("r0", 0, 1, 2, "a0"),
      ("r1", 1, 2, 0, "a1"),
      ("r2", 2, 0, 1, "a2")
    )).toDF("prev_column", "c0", "c1", "c2", "post_column")

    val resultDf = dfIn.withColumn("first",
      when(col("c0").geq(col("c1")).and(col("c0").geq(col("c2"))), "c0")
        .when(col("c1").geq(col("c0")).and(col("c1").geq(col("c2"))), "c1")
        .otherwise("c2")
    ).withColumn("second",
      when(col("c0").between(col("c1"), col("c2"))
        .or(col("c0").between(col("c2"), col("c1"))), "c0")
        .when(col("c1").between(col("c0"), col("c2"))
          .or(col("c1").between(col("c2"), col("c1"))), "c1")
        .otherwise("c2")
    )
    resultDf.show(false)
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
      .groupBy("id2").pivot("id").agg(first("col", ignoreNulls = true)).drop("id2")
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


  def q_74781731(spark: SparkSession) = {
    import spark.implicits._
    val data = Seq(
      Seq("sat", "sun"),
      Seq("mon", "wed"),
      Seq("fri"),
      Seq("fri", "sat"),
      Seq("mon", "sun", "sat")
    )

    val df = spark.sparkContext.parallelize(data).toDF("days")
    val allDf = df.select(explode(col("days")).as("days")).agg(collect_set("days").as("all_days"))
      .withColumn("join_column", lit(1))
    df.withColumn("join_column", lit(1)).join(broadcast(allDf), Seq("join_column"), "left").drop("join_column").show(false)
  }


  def q_74781415(spark: SparkSession) = {
    import spark.implicits._

    val keyDf = spark.sparkContext.parallelize(Seq("Car", "bike", "Bus", "Auto")).toDF("model_name")

    val data = Seq(
      (1, "Honda", "city", "YES", "yamaha", "fz", "school", "ford", "Manual"),
      (2, "TATA", "punch", "YES", "hero", "xtreme", "public", "Ashok", "Gas")
    )
    val InputDf = spark.sparkContext.parallelize(data).toDF("ID", "car_type", "car_name", "car_PRESENT", "bike_type", "bike_name", "bus_type", "bus_name", "auto_type")

    keyDf.distinct().collect().map(row => row.getString(0).toLowerCase()).foreach(r => {
      if (InputDf.columns.map(_.toLowerCase()).containsSlice(List(s"${r}_type", s"${r}_name"))) {
        val df = InputDf.select("ID", s"${r}_type", s"${r}_name")
        df.show(false)
        //      df.write.csv(s"path/.../$r.csv")
      }
    })
  }

  def q_74791827(spark: SparkSession) = {
    import spark.implicits._
    val df1 = spark.sparkContext.parallelize(Seq(("Car", "car"), ("bike", "Rocket"), ("Bus", "BUS"), ("Auto", "Machine"))).toDF("c1", "c2")
    df1.filter(lower(col("c1")).equalTo(lower(col("c2")))).explain()
  }

  def q_74798859(spark: SparkSession) = {
    import spark.implicits._
    val data = Seq(
      (1, "Frankfurt am main", "just test", "Germany"),
      (2, "should do this also", "test", "France")
    )

    val df = spark.sparkContext.parallelize(data).toDF("ID", "City", "test", "Country")

    val dfCapitalized = df.columns.foldLeft(df) {
      (df, column) => df.withColumn(column, initcap(col(column)))
    }
    dfCapitalized.show(false)
  }

  def q_74801937(spark: SparkSession) = {
    def flattenDataframe(df: DataFrame): DataFrame = {

      val fields = df.schema.fields
      val fieldNames = fields.map(x => x.name)
      val length = fields.length

      for (i <- 0 to fields.length - 1) {
        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name
        fieldtype match {
          case arrayType: ArrayType =>
            val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
            val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
            // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
            val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
            return flattenDataframe(explodedDf)
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
            val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
            val explodedf = df.select(renamedcols: _*)
            return flattenDataframe(explodedf)
          case _ =>
        }
      }
      df
    }

    val df = spark.read.option("multiline", "true").json("src/main/resources/input/files/flatten.json")
    val flattendedJSON = flattenDataframe(df)
    df.show(false)
    flattendedJSON.show(false)
  }

  def q_74829974(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      ("1", "r2_test"),
      ("2", "some_other_value"),
      ("3", "hs_2_card"),
      ("4", "vsx_np_v2"),
      ("5", "r2_test"),
      ("2", "some_other_value2"),
      ("7", null)
    ).toDF("id", "my_column")

    val elements = List("r2", "hs", "np")
    val searchUdf = udf((value: String) => elements.exists(value.contains))
    df.filter(col("my_column").isNotNull).filter(row => elements.exists(row.getAs[String]("my_column").contains)).show()
  }

  def q_74984032(spark: SparkSession) = {
    val df = spark.read.option("multiline", "true").json("src/main/resources/input/files/74984032.json")
    df.show(false)
    df.printSchema()
  }

  def q_74704834(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      (1, "2022-12-01", "2022-12-03", 12),
      (2, "2022-12-05", "2022-12-10", 100)
    ).toDF("id", "start_day", "end_date", "sum")

    val w = Window.partitionBy("id").orderBy("date")
    df.withColumn("start_day", col("start_day").cast("date"))
      .withColumn("end_date", date_add(col("end_date").cast("date"), -1))
      .withColumn("datesDiff", datediff(col("end_date"), col("start_day")) + 1)
      .withColumn("date", explode(expr("sequence(start_day, end_date, interval 1 day)")))
      .withColumn("idx", row_number().over(w))
      .withColumn("sum", col("sum").divide(col("datesDiff")))
      .select("id", "idx", "date", "sum")
      .show(false)

  }

  def q_75035551(spark: SparkSession) = {
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load("src/main/resources/input/files/partitions")
      .repartition(col("dhi")).drop("dhi")
    //          .filter(col("dhi").equalTo("2022"))

    df.explain()
    df.write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/output/partitions")
  }

  def q_59238884(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      (1, Seq(1, 2, 3)),
      (2, Seq(4, 5, 6))
    ).toDF("col1", "array")

    df.select(df("id"), df("numbers").getItem(0).as("first_number")).show()
  }

  def q_75072909(spark: SparkSession) = {
    //    val schemaRsvp = new StructType()
    //      .add("place", StructType(Array(
    //        StructField("place_name", DataTypes.StringType),
    //        StructField("lon", DataTypes.IntegerType),
    //        StructField("lat", DataTypes.IntegerType),
    //        StructField("place_id", DataTypes.IntegerType))))
    //      .add("region", StructType(Array(
    //        StructField("place_name", ArrayType(
    //          Array(
    //            StructField("place_name", DataTypes.StringType),
    //            StructField("lon", DataTypes.IntegerType),
    //            StructField("lat", DataTypes.IntegerType),
    //          )
    //        )))))
    val ip = spark.read.json("src/main/resources/input/files/75072909.json")
    ip.show()
    ip.printSchema()
    ip.select(col("place"), explode(col("region.region_issues")).as("region_issues"))
      .select("place.*", "region_issues.*").show(false)
  }

  def q_75088376(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      ("1", "a", "b", "c", "d", "e", "f"),
      ("2", "aa", "bb", "cc", "dd", "ee", "ff")
    ).toDF("id", "key1_suffix1", "key2_suffix1", "suffix1", "key1_suffix2", "key2_suffix2", "suffix2")

    val inputSuffix = Array("suffix1", "suffix2", "suffix3")
    val inputSuffixFiltred = inputSuffix.filter(c => df.columns.contains(s"key1_$c") && df.columns.contains(s"key2_$c") && df.columns.contains(c))
    val tagsCol = inputSuffixFiltred.map(c => struct(s"key1_$c", s"key2_$c", c).as(c))
    val colsToDelete = inputSuffixFiltred.flatMap(c => Seq(s"key1_$c", s"key2_$c", c))
    val res = df.withColumn("tags", struct(tagsCol: _*)).drop(colsToDelete: _*)
    res.printSchema()

  }

  def q_75126205(spark: SparkSession) = {
    val encoderSchema = Encoders.product[ZipCode].schema

    val df = spark.read.format("csv")
      .schema(encoderSchema)
      .option("header", "true")
      .option("badRecordsPath", "src/main/resources/output/75126205/zipcodesBadRecords.csv")
      .load("src/main/resources/input/files/zipcodesBadRecords.csv")
    df.show()
  }

  def q_75141003(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      ("1", "USD", "10"),
      ("1", "EUR", "20"),
      ("2", "USD", "30"),
      ("2", "EUR", "40")
    ).toDF("id", "currency", "value")

    df.show(false)
    df.union(df.withColumn("currency", lit("JPY"))).show(false)
  }

  def q_75266938(spark: SparkSession) = {
    import spark.implicits._
    val df8 = Seq(
      ("2022-08-22 10:00:00", 417.7, 419.97, 419.97, 417.31, "nothing"),
      ("2022-08-22 11:30:00", 417.35, 417.33, 417.46, 416.77, "buy"),
      ("2022-08-22 13:00:00", 417.55, 417.68, 418.04, 417.48, "sell"),
      ("2022-08-22 14:00:00", 417.22, 417.8, 421.13, 416.83, "sell")
    )

    val df77 = spark.createDataset(df8).toDF("30mins_date", "30mins_close", "30mins_open", "30mins_high", "30mins_low", "signal")

    val assembler_features = new VectorAssembler()
      .setInputCols(Array("30mins_close", "30mins_open", "30mins_high", "30mins_low"))
      .setOutputCol("features")

    val output2 = assembler_features.transform(df77)

    val indexer = new StringIndexer()
      .setInputCol("signal")
      .setOutputCol("signalIndex")
    output2.show(false)
    val indexed = indexer.fit(output2).transform(output2)

    val assembler_label = new VectorAssembler()
      .setInputCols(Array("signalIndex"))
      .setOutputCol("signalIndexV")

    val output = assembler_label.transform(indexed)

    val dt = new DecisionTreeClassifier()
      .setLabelCol("features")
      .setFeaturesCol("signalIndexV")

    val Array(trainingData, testData) = output.select("features", "signalIndexV").randomSplit(Array(0.7, 0.3))
    trainingData.show(false)
    trainingData.printSchema()
    val model = dt.fit(trainingData)
  }

  def q_75288662(spark: SparkSession) = {
    val df = spark.read.option("multiline", "true").json("src/main/resources/input/files/75288662.json")
    df.select(col("meta"), explode(col("objects")).as("objects"))
      .select("meta.*", "objects.*")
      .select("a", "b", "caucus", "person.*")
      .show(false)
  }

}
