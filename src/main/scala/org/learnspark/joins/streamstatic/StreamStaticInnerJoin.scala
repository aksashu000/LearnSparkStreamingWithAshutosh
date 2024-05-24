package org.learnspark.joins.streamstatic

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


case class DeviceInfo(id: String, eventTime: String, clickedResource: String, deviceType: String, osVersion: String)

object StreamStaticJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Stream-Static Inner Join Example")
      .getOrCreate()

    val staticUserDetailsSchema = StructType(List(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("location", StringType, nullable = true)
    ))

    val streamingDeviceInfoSchema = StructType(List(
      StructField("id", StringType, nullable = true),
      StructField("eventTime", StringType, nullable = true),
      StructField("clickedResource", StringType, nullable = true),
      StructField("deviceType", StringType, nullable = true),
      StructField("osVersion", StringType, nullable = true)))

    import spark.implicits._

    val staticUserDetailsDF =
      spark
        .read
        .schema(staticUserDetailsSchema)
        .json("src/main/resources/data/joins/streamstatic/user_details.json")

    val bootStrapServer = "localhost:9092"
    val sourceTopic = "device_info"
    val startingOffsets = "latest"

    val streamingDf: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", sourceTopic)
      .option("startingOffsets", startingOffsets)
      .load()

    val deviceInfoStreamingDataset: Dataset[DeviceInfo] =
      streamingDf
      .selectExpr("CAST(value AS STRING) as jsonValue")
      .select(from_json($"jsonValue", schema = streamingDeviceInfoSchema).as("data"))
      .select("data.*")
      .as[DeviceInfo]

    val joinType = "inner"

    val joinedDf =
      deviceInfoStreamingDataset
        .join(staticUserDetailsDF, deviceInfoStreamingDataset("id") === staticUserDetailsDF("id"), joinType)

    joinedDf
      .writeStream
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .outputMode(OutputMode.Update())
      .option("checkPointLocation", "/tmp/data_engineering/" + "_" + java.util.UUID.randomUUID().toString)
      .start()

    spark.streams.awaitAnyTermination()

  }

}
