package com.uef.bigdata
import org.apache.spark.sql.SparkSession
object ReadCSV1 {
  def main(args: Array[String]): Unit = {
    //val filepath=List("data/JSON_Files/read.csv","data/JSON_Files/read.csv" )
    val spark = SparkSession
      .builder()
      .appName("ReadCSV1")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._
    val data = Seq(
      (19,"A", "20-20-2020", 20),
      (20,"B", "20-20-2020", 20),
      (21,"C", "20-20-2020", 20),
      (22,"D", "20-20-2020", 20),
      (23,"E", "20-20-2020", 20)
    ).toDF("id", "name", "dob","age")
    data.write.option("header", value = true).mode("overwrite")
      .csv(path="data/write1.csv")

    val datadf2=spark.read.option("header", value = true).csv(path = "data/write1.csv")

//    val datadf = spark.read
//      .option("header", value=true)
//      .option("delimiter", "|")

      //.csv(paths=List("data/JSON_Files/read.csv","data/JSON_Files/read2.csv") :_*)
   // datadf.printSchema()
    datadf2.show()
    spark.stop()
  }
}
