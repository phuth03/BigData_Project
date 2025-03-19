package com.uef.bigdata
case class emp(name:String,city:String, zipcode:Int)

object CaseClass_Demo {
  def main(args: Array[String]): Unit = {
    val emp1 = emp("John", "New York", 1009)
    val emp2 = emp("Devish", "India", 1234)

    def printEmployeeInfo(employee:emp):Unit = employee match{
      case emp(name, city, zipcode)=> println(s"Name: $name, City: $city, Zipcode: $zipcode")
    }
    printEmployeeInfo(emp1)
    printEmployeeInfo(emp2)
  }
}
