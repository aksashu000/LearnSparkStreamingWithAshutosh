package org.learnspark.windows

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, session_window, to_timestamp, udf}

/*
 We will be using  "src/main/resources/data/sample_data_session_window.txt" file's content to demo this example
*/
object SessionWindow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("File Source Session Window Example").master("local[*]").getOrCreate()

    import spark.implicits._

    val filePath = "src/main/resources/data/sample_data_session_window.txt"

    val sampleDataset = spark
      .read
      .text(filePath)
      .as[String]
      .map(line => (line.split(",")(0), line.split(",")(1)))
      .toDF("eventId", "timestamp")

    sampleDataset.printSchema()

    sampleDataset.show(false)

    val finalDf =
      sampleDataset
      .withColumn("timestamp", to_timestamp($"timestamp"))
      .groupBy($"eventId", session_window($"timestamp", "5 minutes"))
      .count()

    finalDf.printSchema()
    finalDf.show(false)

  }
}
