package com.uef.bigdata
import org.apache.spark.sql.SparkSession

object SQLDF1 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("SQL 1st DATA FRAME")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val data = Seq(
      (12,"A", "20-09-2020", 20),
      (13,"B", "21-09-2020", 21),
      (14,"D", "22-09-2020", 22),
      (15,"D", "23-09-2020", 23)
    ).toDF("id", "Name", "dob", "age")
//    data.show()
//    data.printSchema()
//    data.select($"name",$"age"> 21).show()
//    data.groupBy("name").count().show()

    data.createOrReplaceTempView("people")
    data.createOrReplaceGlobalTempView("people")
//    spark.sql("select name, count(1) from people group by name").show()
    spark.newSession().sql("Select * from global_temp.people").show()
    spark.stop()
  }
}
