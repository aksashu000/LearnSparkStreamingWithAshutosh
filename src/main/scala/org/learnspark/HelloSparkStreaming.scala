package org.learnspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


object HelloSparkStreaming{

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Hello Spark Streaming").master("local[*]").getOrCreate()
    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("eventTime", TimestampType, nullable = false),
      StructField("clickedResource", StringType, nullable = false)
    ))
    val df = sparkSession.readStream.schema(schema).json("src/main/resources/data/")
    df.printSchema()

    df
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }

}