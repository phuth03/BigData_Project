package com.uef.bigdata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.StdIn

import org.apache.spark.sql.types.{DoubleType,IntegerType,StringType,StructType}

object SearchFriendByAge {
  def main(args: Array[String]): Unit = {
    case class People(id: Int, name: String, age: Int, friends: Int)

    val spark = SparkSession.builder()
      .appName("Search Friends by Age")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val filePath = "data/fakefriends-noheader.csv"

    val df = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .toDF("id", "name", "age", "friends") // Assign column names manually

//    // Get user input for age range
//    print("Enter minimum age: ")
//    val minAge = StdIn.readInt()
//    print("Enter maximum age: ")
//    val maxAge = StdIn.readInt()

    df.createOrReplaceTempView("Emp")
//    val teen = spark.sql("SELECT * FROM Emp").collect()
//    teen.foreach(println)
    df.printSchema()
    df.show()
    // Stop Spark session
    spark.stop()
  }
}
