package Final
import scala.io.Source
import scala.collection.mutable.{Map => MutableMap, ListBuffer}
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}
import java.util.regex.Pattern

/**
 * Movie Analytics System
 * Core functionalities:
 * - Count movies by genre
 * - Find top 10 most viewed movies
 * - List latest released movies
 * - Count movies starting with numbers or specific letters
 * - Get distinct list of available genres
 * - CRUD operations for movies
 */
object MovieAnalyticsSystem {

  // Define data structures
  case class Movie(id: Int, title: String, genres: List[String], releaseYear: Int)
  case class Rating(userId: Int, movieId: Int, rating: Int, timestamp: Long)
  case class User(id: Int, gender: String, age: Int, occupation: Int, zipCode: String)

  // File paths
  val movieFilePath = "data/FinalData/movies.dat"
  val ratingsFilePath = "data/FinalData/ratings.dat"
  val usersFilePath = "data/FinalData/users.dat"

  // Storage maps
  val movies = MutableMap[Int, Movie]()
  val ratings = ListBuffer[Rating]()
  val users = MutableMap[Int, User]()

  /**
   * Parse a movie data line from file
   * Format: movieId::title::genres
   */
  def parseMovieLine(line: String): Movie = {
    val fields = line.split("::")
    val id = fields(0).toInt
    val title = fields(1)

    // Extract release year from title
    val yearPattern = Pattern.compile("\\((\\d{4})\\)$")
    val matcher = yearPattern.matcher(title)
    val releaseYear = if (matcher.find()) matcher.group(1).toInt else 0

    val genres = fields(2).split("\\|").toList
    Movie(id, title, genres, releaseYear)
  }

  /**
   * Parse a rating data line from file
   * Format: userId::movieId::rating::timestamp
   */
  def parseRatingLine(line: String): Rating = {
    val fields = line.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toLong)
  }

  /**
   * Parse a user data line from file
   * Format: userId::gender::age::occupation::zipCode
   */
  def parseUserLine(line: String): User = {
    val fields = line.split("::")
    User(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt, fields(4))
  }

  /**
   * Load movie data from file
   */
  def loadMovies(): Unit = {
    try {
      val source = Source.fromFile(movieFilePath)
      for (line <- source.getLines()) {
        val movie = parseMovieLine(line)
        movies(movie.id) = movie
      }
      source.close()
      println(s"Successfully loaded ${movies.size} movies from file.")
    } catch {
      case e: Exception =>
        println(s"Error reading movie file: ${e.getMessage}")
    }
  }

  /**
   * Load ratings data from file
   */
  def loadRatings(): Unit = {
    try {
      val source = Source.fromFile(ratingsFilePath)
      for (line <- source.getLines()) {
        ratings += parseRatingLine(line)
      }
      source.close()
      println(s"Successfully loaded ${ratings.size} ratings from file.")
    } catch {
      case e: Exception =>
        println(s"Error reading ratings file: ${e.getMessage}")
    }
  }

  /**
   * Load users data from file
   */
  def loadUsers(): Unit = {
    try {
      val source = Source.fromFile(usersFilePath)
      for (line <- source.getLines()) {
        val user = parseUserLine(line)
        users(user.id) = user
      }
      source.close()
      println(s"Successfully loaded ${users.size} users from file.")
    } catch {
      case e: Exception =>
        println(s"Error reading users file: ${e.getMessage}")
    }
  }

  /**
   * Save movies data to file
   */
  def saveMovies(): Unit = {
    try {
      val writer = new PrintWriter(new File(movieFilePath))
      for (movie <- movies.values) {
        val genresStr = movie.genres.mkString("|")
        writer.println(s"${movie.id}::${movie.title}::$genresStr")
      }
      writer.close()
      println("Successfully saved movie data to file.")
    } catch {
      case e: Exception =>
        println(s"Error writing to movie file: ${e.getMessage}")
    }
  }

  /**
   * Count movies by genre
   * @return Map with genre as key and movie count as value
   */
  def countMoviesByGenre(): Map[String, Int] = {
    val genreCounts = MutableMap[String, Int]().withDefaultValue(0)

    for (movie <- movies.values) {
      for (genre <- movie.genres) {
        genreCounts(genre) += 1
      }
    }

    genreCounts.toMap
  }

  /**
   * Get distinct list of all available genres
   * @return List of unique genres
   */
  def getDistinctGenres(): List[String] = {
    val genres = for {
      movie <- movies.values
      genre <- movie.genres
    } yield genre

    genres.toList.distinct.sorted
  }

  /**
   * Get top 10 most viewed (rated) movies
   * @return List of tuples (movieId, title, view count)
   */
  def getTopViewedMovies(limit: Int = 10): List[(Int, String, Int)] = {
    // Count ratings for each movie
    val movieViewCounts = ratings.groupBy(_.movieId).map {
      case (movieId, movieRatings) => (movieId, movieRatings.size)
    }

    // Sort by view count and take top N
    movieViewCounts.toList
      .sortBy(-_._2)
      .take(limit)
      .map { case (movieId, count) =>
        (movieId, movies.getOrElse(movieId, Movie(0, "Unknown", List(), 0)).title, count)
      }
  }

  /**
   * List the latest released movies
   * @return List of newest movies based on release year
   */
  def getLatestReleasedMovies(limit: Int = 10): List[(Int, String, Int)] = {
    movies.values.toList
      .sortBy(-_.releaseYear)
      .take(limit)
      .map(movie => (movie.id, movie.title, movie.releaseYear))
  }

  /**
   * Count movies starting with numbers or specific letters
   * @return Map with starting character as key and count as value
   */
  def countMoviesByFirstChar(): Map[String, Int] = {
    val charCounts = MutableMap[String, Int]().withDefaultValue(0)

    for (movie <- movies.values) {
      // Extract the first character of the title
      if (movie.title.nonEmpty) {
        val firstChar = movie.title.charAt(0).toString.toUpperCase

        // Group digits together
        val key = if (firstChar.matches("\\d")) "0-9" else firstChar
        charCounts(key) += 1
      }
    }

    charCounts.toMap
  }

  /**
   * Add a new movie to the collection
   */
  def addMovie(id: Int, title: String, genres: List[String], releaseYear: Int): Boolean = {
    if (movies.contains(id)) {
      println(s"Movie with ID $id already exists.")
      false
    } else {
      movies(id) = Movie(id, title, genres, releaseYear)
      saveMovies()
      println(s"Added movie: $title")
      true
    }
  }

  /**
   * Delete a movie from the collection
   */
  def deleteMovie(id: Int): Boolean = {
    if (movies.contains(id)) {
      val title = movies(id).title
      movies.remove(id)
      saveMovies()
      println(s"Deleted movie: $title (ID: $id)")
      true
    } else {
      println(s"Movie with ID $id not found.")
      false
    }
  }

  /**
   * Update movie information
   */
  def updateMovie(id: Int, title: String = null, genres: List[String] = null, releaseYear: Int = -1): Boolean = {
    if (movies.contains(id)) {
      val currentMovie = movies(id)
      val updatedTitle = if (title == null) currentMovie.title else title
      val updatedGenres = if (genres == null) currentMovie.genres else genres
      val updatedYear = if (releaseYear == -1) currentMovie.releaseYear else releaseYear

      movies(id) = Movie(id, updatedTitle, updatedGenres, updatedYear)
      saveMovies()
      println(s"Updated movie ID $id: $updatedTitle")
      true
    } else {
      println(s"Movie with ID $id not found.")
      false
    }
  }

  /**
   * Display genre counts
   */
  def displayGenreCounts(): Unit = {
    val genreCounts = countMoviesByGenre()
    println("\n=== Number of Movies by Genre ===")

    // Sort results by count in descending order
    val sortedCounts = genreCounts.toSeq.sortBy(-_._2)

    for ((genre, count) <- sortedCounts) {
      println(f"$genre%-20s $count%d")
    }

    println(s"\nTotal genres: ${genreCounts.size}")
    println(s"Total movies: ${movies.size}")
  }

  /**
   * Display movies starting with different characters
   */
  def displayMoviesByFirstChar(): Unit = {
    val charCounts = countMoviesByFirstChar()
    println("\n=== Movies by Starting Character ===")

    // Sort alphabetically with numbers first
    val sortedCounts = charCounts.toSeq.sortBy(_._1)

    for ((char, count) <- sortedCounts) {
      println(f"$char%-5s $count%d")
    }

    println(s"\nTotal different starting characters: ${charCounts.size}")
  }

  /**
   * Display top viewed movies
   */
  def displayTopViewedMovies(limit: Int = 10): Unit = {
    val topMovies = getTopViewedMovies(limit)
    println(s"\n=== Top $limit Most Viewed Movies ===")

    if (topMovies.isEmpty) {
      println("No ratings data available.")
    } else {
      println(f"${"Rank"}%-5s ${"ID"}%-5s ${"Title"}%-50s ${"Views"}%-6s")
      println("-" * 70)

      topMovies.zipWithIndex.foreach { case ((id, title, views), index) =>
        println(f"${index+1}%-5d $id%-5d $title%-50s $views%-6d")
      }
    }
  }

  /**
   * Display latest released movies
   */
  def displayLatestMovies(limit: Int = 10): Unit = {
    val latestMovies = getLatestReleasedMovies(limit)
    println(s"\n=== $limit Latest Released Movies ===")

    if (latestMovies.isEmpty) {
      println("No movie data available.")
    } else {
      println(f"${"Rank"}%-5s ${"ID"}%-5s ${"Title"}%-50s ${"Year"}%-6s")
      println("-" * 70)

      latestMovies.zipWithIndex.foreach { case ((id, title, year), index) =>
        println(f"${index+1}%-5d $id%-5d $title%-50s $year%-6d")
      }
    }
  }

  /**
   * Display distinct genres
   */
  def displayDistinctGenres(): Unit = {
    val genres = getDistinctGenres()
    println("\n=== Distinct Movie Genres ===")

    if (genres.isEmpty) {
      println("No genre data available.")
    } else {
      for (genre <- genres) {
        println(genre)
      }

      println(s"\nTotal distinct genres: ${genres.size}")
    }
  }

  /**
   * Main method to run the program
   */
  def main(args: Array[String]): Unit = {
    loadMovies()
    loadRatings()
    loadUsers()

    var running = true
    while (running) {
      println("\n=== MOVIE ANALYTICS SYSTEM ===")
      println("1. Show number of movies by genre")
      println("2. Show top 10 most viewed movies")
      println("3. List latest released movies")
      println("4. Count movies by starting character")
      println("5. Show distinct list of genres")
      println("6. Add new movie")
      println("7. Update movie information")
      println("8. Delete movie")
      println("0. Exit")

      print("\nEnter your choice: ")
      val choice = scala.io.StdIn.readLine()

      choice match {
        case "1" =>
          displayGenreCounts()

        case "2" =>
          print("Enter number of movies to display (default 10): ")
          val input = scala.io.StdIn.readLine()
          val limit = Try(input.toInt).getOrElse(10)
          displayTopViewedMovies(limit)

        case "3" =>
          print("Enter number of movies to display (default 10): ")
          val input = scala.io.StdIn.readLine()
          val limit = Try(input.toInt).getOrElse(10)
          displayLatestMovies(limit)

        case "4" =>
          displayMoviesByFirstChar()

        case "5" =>
          displayDistinctGenres()

        case "6" =>
          try {
            print("Enter movie ID: ")
            val id = scala.io.StdIn.readLine().toInt
            print("Enter movie title: ")
            val title = scala.io.StdIn.readLine()
            print("Enter release year: ")
            val releaseYear = scala.io.StdIn.readLine().toInt
            print("Enter genres (separated by |): ")
            val genresInput = scala.io.StdIn.readLine()
            val genres = genresInput.split("\\|").toList

            addMovie(id, title, genres, releaseYear)
          } catch {
            case e: Exception => println(s"Error: ${e.getMessage}")
          }

        case "7" =>
          try {
            print("Enter movie ID to update: ")
            val id = scala.io.StdIn.readLine().toInt

            if (movies.contains(id)) {
              print(s"Enter new title (Enter to keep '${movies(id).title}'): ")
              val titleInput = scala.io.StdIn.readLine()
              val title = if (titleInput.isEmpty) null else titleInput

              print(s"Enter new release year (Enter to keep ${movies(id).releaseYear}): ")
              val yearInput = scala.io.StdIn.readLine()
              val releaseYear = if (yearInput.isEmpty) -1 else yearInput.toInt

              print(s"Enter new genres (separated by |, Enter to keep '${movies(id).genres.mkString("|")}'): ")
              val genresInput = scala.io.StdIn.readLine()
              val genres = if (genresInput.isEmpty) null else genresInput.split("\\|").toList

              updateMovie(id, title, genres, releaseYear)
            } else {
              println(s"Movie with ID $id not found.")
            }
          } catch {
            case e: Exception => println(s"Error: ${e.getMessage}")
          }

        case "8" =>
          try {
            print("Enter movie ID to delete: ")
            val id = scala.io.StdIn.readLine().toInt
            deleteMovie(id)
          } catch {
            case e: Exception => println(s"Error: ${e.getMessage}")
          }

        case "0" =>
          running = false
          println("Thank you for using the Movie Analytics System!")

        case _ =>
          println("Invalid choice. Please try again.")
      }
    }
  }
}