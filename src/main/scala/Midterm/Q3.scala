package Midterm
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class CustomerOrder(customerID: Int, itemID: Int, amountSpent: Double)

object Q3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TotalAmountSpentPerCustomer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.textFile("data/customer-orders.csv")

    val customerOrdersDS = data.map { line =>
      val fields = line.split(",")
      val customerID = fields(0).toInt
      val itemID = fields(1).toInt
      val amountSpent = fields(2).toDouble
      CustomerOrder(customerID, itemID, amountSpent)
    }

    val totalAmountSpentPerCustomer = customerOrdersDS.groupBy("customerID")
      .agg(sum("amountSpent").as("totalAmountSpent"))

    totalAmountSpentPerCustomer.show()

    spark.stop()
  }
}
