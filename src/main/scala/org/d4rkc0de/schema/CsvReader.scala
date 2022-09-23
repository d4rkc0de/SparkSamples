package org.d4rkc0de.schema

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.d4rkc0de.common.SparkFactory
import org.d4rkc0de.models.ZipCode

object CsvReader {
  def main(args: Array[String]) = {

    val spark = SparkFactory.getSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    val encoderSchema = Encoders.product[ZipCode].schema
    encoderSchema.printTreeString()

    var df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load("src/main/resources/input/files/zipcodes.csv")
    df.printSchema()
    df = applySchemaToDataFrame(df, encoderSchema)
    df.printSchema()
  }

  def applySchemaToDataFrame(df: DataFrame, schema: StructType): DataFrame = {
    var tmp = df
    schema.foreach(field => tmp = tmp.withColumn(field.name, col(field.name).cast(field.dataType)))
    tmp
  }
}
