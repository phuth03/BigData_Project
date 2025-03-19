package com.uef.bigdata
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object Revenue {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Revenue Prediction")
      .master("local[*]") // Using local mode for demo
      .getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1.2,1000.0),
      (1.8, 1500.0),
      (2.0, 2100.0),
      (2.5,2100.0),
      (3.0,2500.0),
      (3.5,2700.0),
      (4.0,3000.0)
    ).toDF("page_speed", "revenue")

    val assembler = new VectorAssembler()
      .setInputCols(Array("page_speed"))
      .setOutputCol("features")
    val transformedData = assembler.transform(data).select("features", "revenue")

    val lr = new LinearRegression()
      .setLabelCol("revenue")
      .setFeaturesCol("features")
    val model = lr.fit(transformedData)

    println(s"Coefficients: ${model.coefficients}, Intercept: ${model.intercept}")


    val predictions = model.transform(transformedData)
    predictions.show()

    spark.stop()
  }
}

