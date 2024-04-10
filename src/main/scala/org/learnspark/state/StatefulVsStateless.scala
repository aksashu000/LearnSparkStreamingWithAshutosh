package org.learnspark.state

import org.apache.spark.sql.functions.{from_json, to_timestamp, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class DeviceInfo(id: String, eventTime: String, clickedResource: String, deviceType: String, osVersion: String)

object StatefulVsStateless {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Stateful vs Stateless MultiQuery Example")
      .getOrCreate()

    val bootStrapServer = "localhost:9092"
    val sourceTopic = "device_info"
    val startingOffsets = "latest"
    val destTopic = "iPhone_devices"

    val jsonFormatSchema = StructType(List(
      StructField("id", StringType, nullable = true),
      StructField("eventTime", StringType, nullable = true),
      StructField("clickedResource", IntegerType, nullable = true),
      StructField("deviceType", StringType, nullable = true),
      StructField("osVersion", IntegerType, nullable = true)))

    import spark.implicits._

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", sourceTopic)
      .option("startingOffsets", startingOffsets)
      .load()

    val deviceInfo: Dataset[DeviceInfo] = df
      .selectExpr("CAST(value AS STRING) as jsonValue")
      .select(from_json($"jsonValue", schema = jsonFormatSchema).as("data"))
      .select("data.*")
      .as[DeviceInfo]

    //This is Stateful operation
    deviceInfo
      .withColumn("eventTime", to_timestamp($"eventTime"))
      .withWatermark("eventTime", "15 minutes")
      .groupBy(window($"eventTime", "15 minutes"), $"id")
      .count()
      .writeStream
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .outputMode(OutputMode.Update())
      .option("checkPointLocation", "/tmp/data_engineering/" + "_" + java.util.UUID.randomUUID().toString)
      .start()

    // Write the records having iPhone deviceType to the Kafka topic iPhone_devices
    // This is Stateless operation
    deviceInfo
      .filter(row => row.deviceType == "iPhone")
      .selectExpr("id as key",
        "to_json(named_struct('id', id, 'eventTime', eventTime, 'clickedResource', clickedResource, 'deviceType', " +
          "deviceType, 'osVersion', osVersion)) as value")
      .writeStream
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("topic", destTopic)
      .option("checkPointLocation", "/tmp/data_engineering/"+ destTopic + "_" + java.util.UUID.randomUUID().toString)
      .outputMode(OutputMode.Append())
      .start()

    spark.streams.awaitAnyTermination()

  }
}
