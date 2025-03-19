package com.uef.bigdata

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Assignment3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FileStreamingApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(Array(
      StructField("Item_Id", IntegerType, true),
      StructField("Item_name", StringType, true),
      StructField("Item_price", DoubleType, true),
      StructField("Item_qty", IntegerType, true),
      StructField("date", DateType, true)
    ))

    val fileStreamDF = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv("data/DATA_ASSIGNMENT3")

    val withFilenameDF = fileStreamDF.withColumn("fullPath", input_file_name())
      .withColumn("FName", element_at(split(col("fullPath"), "/"), -1))

    val processedDF = withFilenameDF
      .withColumn("Total_Cost", col("Item_price") * col("Item_qty"))
      .groupBy("FName")
      .agg(
        first("date").alias("sDate"),
        current_timestamp().alias("timestamp"),
        sum("Total_Cost").alias("Total_Cost")
      )
      .selectExpr("FName", "sDate", "timestamp", "Total_Cost")

    val query = processedDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoint/assignment3")
      .start()

    query.awaitTermination()  }
}
