package org.learnspark.streamingsources

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

/**
 * This is the example for socket source with complete output mode
 * @author Ashutosh kumar
 */
object SocketSourceWithCompleteOutputMode {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
      .builder()
        .appName("Socket Source Complete Output Mode")
        .master("local[*]")
        .getOrCreate()

    val df: DataFrame = spark
      .readStream
      .format("socket")
      .options(Map("host" -> "localhost", "port" -> "8888"))
      .load()

    df.printSchema()

    import spark.implicits._

    val ds: Dataset[String] = df.as[String]

    val wordCountDf: DataFrame = ds
        .flatMap(_.split(" "))
        .groupBy("value")
        .count()

    val query: StreamingQuery = wordCountDf
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()
  }
}
