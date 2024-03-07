package org.learnspark.streamingsources

import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, udf, window}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter

/*
Append output mode NOT supported when there are streaming aggregations
on streaming DataFrames/DataSets without watermark
*/
case class Event(eventTime: Timestamp, line: String )

object SocketSourceWithAppendOutputMode {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Hello Spark Streaming").master("local[*]").getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("socket")
      .options(Map("host" -> "localhost", "port" -> "8888"))
      .load()

    df.printSchema()

    val events = df
      .as[String]
      .map(x => ((x.split(",")(0)), x.split(",")(1)))
      .toDF("timestamp", "line")


    val query = events
      .withColumn("epochTime", convertToTimeStamp(col("timestamp")))
      .withWatermark("epochTime", "2 minutes")
      .groupBy(window($"epochTime", "60 seconds"), $"line")
      .count()
      .writeStream
      .outputMode(OutputMode.Complete())
      .foreachBatch((df: DataFrame, batchId: Long) => {
        df.show(false)
      })
      .start()


    query.awaitTermination()
  }

  private val convertToTimeStamp = udf((col: String) => {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val localDate = LocalDate.parse(col, formatter)
    new Timestamp(localDate.toEpochDay)
  })

}
