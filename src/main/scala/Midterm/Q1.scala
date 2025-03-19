package Midterm
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

case class SuperheroGraph(id: Int, connections: Array[Int])
case class SuperheroName(id: Int, name: String)
case class SuperheroPopularity(id: Int, name: String, coAppearances: Long)

object Q1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MostPopularSuperhero")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val namesDS: Dataset[SuperheroName] = spark.read
      .textFile("data/Marvel-names.txt")
      .map(line => {
        val fields = line.split("\"")
        val id = fields(0).trim.toInt
        val name = if (fields.length > 1) fields(1) else ""
        SuperheroName(id, name)
      })

    val graphDS: Dataset[SuperheroGraph] = spark.read
      .textFile("data/Marvel-graph.txt")
      .map(line => {
        val fields = line.split("\\s+")
        val id = fields(0).toInt
        val connections = fields.tail.map(_.toInt)
        SuperheroGraph(id, connections)
      })

    val coAppearancesDS = graphDS.map(hero => (hero.id, hero.connections.length.toLong))
      .toDF("id", "coAppearances")
      .as[(Int, Long)]

    val superheroesByPopularityDS = coAppearancesDS
      .join(namesDS, "id")
      .as[(Int, Long, String)]
      .map { case (id, coAppearances, name) =>
        SuperheroPopularity(id, name, coAppearances)
      }

    val mostPopular = superheroesByPopularityDS
      .orderBy(desc("coAppearances"))
      .first()

    println(s"'${mostPopular.name}' is the most popular superhero with ${mostPopular.coAppearances} co-appearances")

    spark.stop()
  }
}
