package com.uef.bigdata
import scala.io.Source
import scala.collection.mutable

object Assignment2 {
  def main(args: Array[String]): Unit = {

    val filePath = "data/Assignment2.txt"
    val topN = 10

    val source = Source.fromFile(filePath)
    val wordCounts = mutable.Map[String, Int]().withDefaultValue(0)

    for (line <- source.getLines()) {
      val words = line.toLowerCase.replaceAll("[^a-zA-Z0-9 ]", "").split(" ").filter(_.nonEmpty)
      words.foreach(word => wordCounts(word) += 1)
    }

    source.close()

    val sortedWordCounts = wordCounts.toSeq.sortBy(-_._2).take(topN)
    println("Top " + topN + " words by frequency:")
    sortedWordCounts.foreach { case (word, count) => println(s"$word: $count") }
  }
}
