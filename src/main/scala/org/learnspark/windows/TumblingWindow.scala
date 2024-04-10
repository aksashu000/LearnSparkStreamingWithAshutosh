package org.learnspark.windows

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, to_timestamp, udf, window}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

object TumblingWindow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Kafka Source Tumbling Window Example").master("local[*]").getOrCreate()

    val bootStrapServer = "localhost:9092"
    val topic = "windows_demo"
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

    val getData =  udf((line:String, index:Int) => line.split(",")(index))

    val wordCountDf: DataFrame =
      df
        .withColumn("timestamp", getData($"value",lit(0)))
        .withColumn("word", getData($"value",lit(1)))
        .drop("value")

    wordCountDf.printSchema()

    val windowedCounts =
      wordCountDf
        .withColumn("timestamp", to_timestamp($"timestamp"))
        .groupBy(window($"timestamp", "10 minutes"), $"word")
        .count()

    val query: StreamingQuery =
      windowedCounts
        .writeStream
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .outputMode(OutputMode.Update())
        .format("console")
        .option("truncate", value = false)
        .option("checkPointLocation", "/tmp/"+java.util.UUID.randomUUID())
        .start()

    query.awaitTermination()
  }
}
