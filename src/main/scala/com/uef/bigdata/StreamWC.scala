package com.uef.bigdata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamWC extends Serializable{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FileStreamingApp")
      .master("local[*]") // Run locally
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()

    import spark.implicits._

    // Define schema for CSV data
    val schema = StructType(Array(
      StructField("Item_Id", IntegerType, true),
      StructField("Item_name", StringType, true),
      StructField("Item_price", DoubleType, true),
      StructField("Item_qty", IntegerType, true),
      StructField("date", DateType, true)
    ))

    // Read streaming CSV files from directory
    val fileStreamDF = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv("data/DATA_ASSIGNMENT3") // Watch folder

    // Extract just the file name (not full path)
    val withFilenameDF = fileStreamDF.withColumn("FullPath", input_file_name())
      .withColumn("FName", substring_index(col("FullPath"), "/", -1))
      .drop("FullPath")

    // Compute total cost
    val processedDF = withFilenameDF
      .withColumn("Total_Cost", col("Item_price") * col("Item_qty"))
      .groupBy("FName")
      .agg(
        first("date").alias("sDate"),
        current_timestamp().alias("timestamp"),
        sum("Total_Cost").alias("Total_Cost")
      )
      .selectExpr("FName", "sDate", "timestamp", "Total_Cost")

    // Write output to console with checkpoint
    val query = processedDF.writeStream
      .outputMode("complete") // Maintain complete state of all processed files
      .format("console") // Display output
      .option("truncate", "false") // Prevent column truncation
      .option("checkpointLocation", "check-point-dir") // Add checkpoint location
      .start()

    query.awaitTermination()

  }
}
