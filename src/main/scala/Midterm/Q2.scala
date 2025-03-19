package Midterm
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class WeatherData(stationID: String, date: String, measureType: String, temperature: Int)

object Q2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MinTemperatureByStation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.textFile("data/1800.csv")

    val weatherDS = data.map { line =>
      val fields = line.split(",")
      val stationID = fields(0)
      val date = fields(1)
      val measureType = fields(2)
      val temperature = fields(3).toInt
      WeatherData(stationID, date, measureType, temperature)
    }.filter(_.measureType == "TMIN")

    val minTemps = weatherDS.groupBy("stationID")
      .agg(min("temperature").as("minTemperature"))

    minTemps.show()

    spark.stop()

  }
}
