package org.learnspark.concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

//Define the case class for typed conversion
case class ClickStream(id:String, eventTime:String, clickedResource:String)

object SparkImplicits {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Hello Spark Streaming").master("local[*]").getOrCreate()

    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("eventTime", TimestampType, nullable = false),
      StructField("clickedResource", StringType, nullable = false)
    ))

    //Encoders => responsible for converting between JVM objects and tabular representation.
    import sparkSession.implicits._

    val clickStreamDataFrame = sparkSession.read.schema(schema).json("src/main/resources/data")

    clickStreamDataFrame.filter(x => x.getString(2) == "addToCart").show(10, truncate = false)

    clickStreamDataFrame.orderBy(col("id").asc).show(20, truncate = false)

    clickStreamDataFrame.orderBy($"id".asc).show(20, truncate = false)

    //Create a Dataset from a DataFrame
    /* Dataset: =>
    1. type-safe, object-oriented programming interface => Spark 1.6
    2. compile-time type safety - production applications can be checked for errors before they are run.
     */
    val clickStreamDataset = clickStreamDataFrame.as[ClickStream]

    clickStreamDataset.filter(x => x.clickedResource == "addToCart").show(10, truncate = false)

  }
}
