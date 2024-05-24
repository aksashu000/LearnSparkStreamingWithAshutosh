package org.learnspark.joins.streamstream

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{expr, from_json, to_timestamp}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class Impressions(impressionAdId: String, impressionTime: String, impressionName: String)
case class UserClicks(userId: String, clickAdId: String, clickTime: String)

object StreamStreamJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Stream-Stream Inner Join Example")
      .getOrCreate()

    import spark.implicits._

    val userClicksSchema = StructType(List(
      StructField("userId", StringType, nullable = true),
      StructField("clickAdId", StringType, nullable = true),
      StructField("clickTime", StringType, nullable = true)
    ))

    val impressionsSchema = StructType(List(
      StructField("impressionAdId", StringType, nullable = true),
      StructField("impressionTime", StringType, nullable = true),
      StructField("impressionName", StringType, nullable = true)
    ))

    val bootStrapServer = "localhost:9092"
    val impressionsTopic = "impressions"
    val userClicksTopic = "user_clicks"
    val startingOffsets = "latest"

    val impressionsStreamingDf: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", impressionsTopic)
      .option("startingOffsets", startingOffsets)
      .load()

    val impressionsStreamingDataset: Dataset[Impressions] =
      impressionsStreamingDf
        .selectExpr("CAST(value AS STRING) as jsonValue")
        .select(from_json($"jsonValue", schema = impressionsSchema).as("data"))
        .select("data.*")
        .withColumn("impressionTime", to_timestamp($"impressionTime"))
        .withWatermark("impressionTime", "60 minutes")
        .as[Impressions]

    val userClicksStreamingDf: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", userClicksTopic)
      .option("startingOffsets", startingOffsets)
      .load()

    val userClicksStreamingDataset: Dataset[UserClicks] =
      userClicksStreamingDf
        .selectExpr("CAST(value AS STRING) as jsonValue")
        .select(from_json($"jsonValue", schema = userClicksSchema).as("data"))
        .select("data.*")
        .withColumn("clickTime", to_timestamp($"clickTime"))
        .withWatermark("clickTime", "120 minutes")
        .as[UserClicks]

    val joinType = "inner"
    val joinExpr =
      """
        |clickAdId = impressionAdId AND
        |clickTime >= impressionTime AND
        |clickTime <= impressionTime + interval 1 hour
        |""".stripMargin

    val joinedDf = impressionsStreamingDataset.join(userClicksStreamingDataset, expr(joinExpr), joinType)

    joinedDf
      .writeStream
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .outputMode(OutputMode.Append())
      .option("checkPointLocation", "/tmp/data_engineering/" + "_" + java.util.UUID.randomUUID().toString)
      .start()

    spark.streams.awaitAnyTermination()

  }
}
