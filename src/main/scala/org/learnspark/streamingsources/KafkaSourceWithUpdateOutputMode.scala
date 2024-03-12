package org.learnspark.streamingsources

import org.apache.spark.sql.functions.{lit, to_timestamp, udf, window}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

case class ClickStream(id:String, eventTime:String, clickedResource:String, deviceType:String, osVersion:String)

object KafkaSourceWithUpdateOutputMode {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Kafka Source with Update Output Mode").master("local[*]").getOrCreate()

    val bootStrapServer = "localhost:9092"
    val topic = "clickstream"
    val startingOffsets = "latest"

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .load()
      .selectExpr("CAST(value AS STRING) as value")

    df.printSchema()

    val getTimestamp =  udf((line:String, index:Int) => line.split(",")(index))

    val wordCountDf: DataFrame =
      df
        .withColumn("timestamp", getTimestamp($"value",lit(0)))
        .withColumn("word", getTimestamp($"value",lit(1)))
        .drop("value")

    wordCountDf.printSchema()

    val windowedCounts =
      wordCountDf
        .withColumn("timestamp", to_timestamp($"timestamp"))
        .withWatermark("timestamp", "10 minutes")
        .groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word")
        .count()

    val query: StreamingQuery =
      windowedCounts
        .writeStream
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .outputMode(OutputMode.Update())
        .format("console")
        .option("truncate", value = false)
        .start()

    query.awaitTermination()
  }

}
