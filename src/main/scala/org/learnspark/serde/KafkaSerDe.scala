package org.learnspark.serde

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

case class SuperHeroes(Id: String, name: String, age: String, DeptName: String, yearOfAdmission: String)

object KafkaSerDe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("KafkaSerDe With MultiQuery Streams")
      .getOrCreate()

    val bootStrapServer = "localhost:9092"
    val sourceTopic = "super_heroes"
    val destTopicDC = "DC_heroes"
    val destTopicMarvel = "Marvel_heroes"

    val startingOffsets = "latest"

    val jsonFormatSchema = StructType(List(
      StructField("Id", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Age", IntegerType, nullable = true),
      StructField("DeptName", StringType, nullable = true),
      StructField("YearOfAdmission", IntegerType, nullable = true)))

    import spark.implicits._

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", sourceTopic)
      .option("startingOffsets", startingOffsets)
      .load()

    val deSerializedDataset: Dataset[SuperHeroes] = df
      .selectExpr("CAST(value AS STRING) as jsonValue")
      .select(from_json($"jsonValue", schema = jsonFormatSchema).as("data"))
      .select("data.*")
      .as[SuperHeroes]

    //Write the Marvel Heroes to Kafka topic Marvel_heroes
    deSerializedDataset
      .filter(row => row.DeptName == "Marvel")
      .selectExpr("Id as key",
        "to_json(named_struct('Id', Id, 'Name', Name, 'Age', Age, 'DeptName', DeptName, 'YearOfAdmission', YearOfAdmission)) as value")
      .writeStream
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("topic", destTopicMarvel)
      .option("checkPointLocation", "/tmp/data_engineering/"+ destTopicMarvel + "_" + java.util.UUID.randomUUID().toString)
      .outputMode(OutputMode.Append())
      .start()

    //Write the DC Heroes to Kafka topic DC_heroes
    deSerializedDataset
      .filter(row => row.DeptName == "DC")
      .selectExpr("Id as key",
        "to_json(named_struct('Id', Id, 'Name', Name, 'Age', Age, 'DeptName', DeptName, 'YearOfAdmission', YearOfAdmission)) as value")
      .writeStream
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("topic", destTopicDC)
      .option("checkPointLocation", "/tmp/data_engineering/"+ destTopicDC + "_" + java.util.UUID.randomUUID().toString)
      .outputMode(OutputMode.Append())
      .start()

    spark.streams.awaitAnyTermination()

  }
}
