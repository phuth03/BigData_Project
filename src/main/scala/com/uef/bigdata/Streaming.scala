package com.uef.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Streaming {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder
      .appName("Streaming")
      .master("local[1]")
      .getOrCreate()
    val df=spark.readStream
      .format("rate")
      .option("rowsPerSecond",3)
      .load()
    df.printSchema()
    println(" Streaming DataFrame:" + df.isStreaming)
    val finaldf=df.withColumn("result",col("value")+lit(1))
    finaldf.printSchema()
    finaldf.writeStream.format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

    spark.stop()
  }

}
