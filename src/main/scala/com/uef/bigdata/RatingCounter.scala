package com.uef.bigdata
import org.apache.log4j._
import org.apache.spark._


object RatingCounter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "RatingCounter")
    val lines = sc.textFile("data/ml-100k/u.data")
    val ratings = lines.map(x => x.split("\t")(2))
    val results = ratings.countByValue()
    val sortedResult = results.toSeq.sortBy(_._1)
    sortedResult.foreach(println)
  }
}
