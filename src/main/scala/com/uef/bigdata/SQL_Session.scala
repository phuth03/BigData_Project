package com.uef.bigdata
import org.apache.spark.sql.SparkSession
object SQL_Session {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[2]")
      .appName("SQL Session")
      .getOrCreate()

    println("App name is: "+spark.sparkContext.appName)
    println("Master is: "+ spark.sparkContext.master)
    spark.stop();
  }
}
