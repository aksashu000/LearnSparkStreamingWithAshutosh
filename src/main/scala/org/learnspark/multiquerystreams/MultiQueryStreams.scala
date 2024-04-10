package org.learnspark.multiquerystreams

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object MultiQueryStreams {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Multi Query Streams").master("local[*]").getOrCreate()

    val bootStrapServer = "localhost:9092"
    val topic = "multi_query_streams"

    //Create a DataFrame from "rate" source
    val df = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()

    import spark.implicits._

    df.printSchema()

    //Write to console => value & double_value
    df
      .withColumn("double_value", $"value" * 2)
      .select("value", "double_value")
      .writeStream
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .format("console")
      .option("truncate", value = false)
      .outputMode(OutputMode.Append())
      .start()

    //Write to Kafka => value & triple_value
    df
      .withColumn("triple_value", $"value" * 3)
      .selectExpr("CAST(value AS STRING) as key", "CAST(triple_value AS STRING) as value")
      .writeStream
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("topic", topic)
      .option("checkPointLocation", "/tmp/"+java.util.UUID.randomUUID().toString)
      .outputMode(OutputMode.Append())
      .start()


    //Start all the streams
    spark.streams.awaitAnyTermination()
  }
}
