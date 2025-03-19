package com.uef.bigdata
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark._

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "RatingCounter")
    val input = sc.textFile("data/book.txt")
    val words = input.flatMap(x=>x.split("\\W+"))
    val lowercaseWords = words.map(x=> x.toLowerCase())
    val wordCounts = lowercaseWords.countByValue()
    wordCounts.foreach(println)
  }
}
