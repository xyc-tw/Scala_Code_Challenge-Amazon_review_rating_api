import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time._
import java.time.format.DateTimeFormatter
import scala.io.StdIn
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

// Libraries for json
import io.circe.{ Decoder, Json }
import io.circe.parser.decode
import io.circe.generic.semiauto._

object ApiServer {

  case class Review(asin: String, helpful: Array[Long], overall: Double, reviewText: String, reviewerID: String, reviewerName: String, summary: String, unixReviewTime: Long)

  /** Transfer the input StringTime to UnixTime */
  private def stringToUnixTime(pos: String, stringTime: String): Long = {
    val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")
    val unixTime = LocalDate.parse(stringTime, formatter).atStartOfDay(ZoneOffset.UTC).toEpochSecond

    if (pos == "start") unixTime else unixTime + 86400
  }

  /** Filter data according to the request */
  private def filter_data(ds: Dataset[Review], start: String, end: String, limit: Int, min_number_reviews: Int): String = {

    /**  Step 1: filter time */
    val timeFiltered = ds.filter(review => review.unixReviewTime >= stringToUnixTime("start", start)
      && review.unixReviewTime < stringToUnixTime("end", end))

    /**  Step 2: group by review id and count the average */
    val asinGrouped = timeFiltered.groupBy("asin")
      .agg(count("asin").as("count"), avg("overall").as("average_rating"))

    /**  Step 3: filter min_number_reviews */
    val minFiltered = asinGrouped.filter(col("count") >= min_number_reviews)

    /**  Step 4: sort according to average_rating, and only limit data as required*/
    val resultDf = minFiltered.orderBy(col("average_rating").desc, col("count").desc).limit(limit)

    /**  Step 5: convert result dataset to JSON */
    val resultJson = resultDf.select(col("asin"), col("average_rating"))
      .toJSON.collect().mkString("[", ",", "]") //DataFrame -> RDD[String] -> Array[String] -> String

    resultJson
  }

  def main(args: Array[String]) {
    /** set file path as argument */
    if (args.length < 1) {
      println("Please provide the path to the JSON file as the first argument.")
      sys.exit(1)
    }
    val path = args(0)

    /** Create a SparkSession */
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("JSON to Dataset")
      .getOrCreate()

    /** read the JSON file and create a DataSet */
    import spark.implicits._
    val reviewDs = spark.read
      .json(path)
      .as[Review]

    /** set HttpHandler */
    object GetViewsHandler extends HttpHandler {

      case class RequestBody(start: String, end: String, limit: Int, min_number_reviews: Int)
      implicit val decoder: Decoder[RequestBody] = deriveDecoder[RequestBody]


      def handle(exchange: HttpExchange) {
        val requestMethod = exchange.getRequestMethod.toUpperCase()
        val response = requestMethod match {
          case "GET" =>
            val path = exchange.getRequestURI.getPath
            path match {
              case "/" =>
                val jsonBody = scala.io.Source.fromInputStream(exchange.getRequestBody).mkString // input stream -> scala.io.BufferedSource -> String
                val result = decode[RequestBody](jsonBody) // json String -> RequestBody
                result match {
                  case Right(request) => {
                    try {
                      filter_data(reviewDs, request.start, request.end, request.limit, request.min_number_reviews)
                    } catch {
                      case e: Exception => e.getMessage()
                    }
                  }
                  case Left(error) => s"Error parsing JSON: $error"
                }
              case _ => "please check your route"
            }
          case _ => "405 Method Not Allowed"
        }

        exchange.getResponseHeaders().set("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length)
        val outputStream = exchange.getResponseBody
        outputStream.write(response.getBytes(StandardCharsets.UTF_8))
        outputStream.close()
      }
    }

    /** create and run Server */
    val server = HttpServer.create(new InetSocketAddress(8080), 0)
    server.createContext("/", GetViewsHandler)
    server.setExecutor(null)
    server.start()
    println("Server is listening on port 8080")
    StdIn.readLine("Press Ctrl + C to stop the server\n")
  }
}