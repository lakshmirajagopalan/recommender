import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.{SparkConf, SparkContext}

case class UserInterests(profileId: String, interests: List[String])
case class Story(storyId: String, title: String, categories: List[String])
case class UserReadHistory(profileId: String, storyId: String)
import akka.http.scaladsl.server.Directives._

object Runner extends App with Formats {

  val modelFilePath =
    "src/main/resources/modelPath"

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Recommender")

  val sc = new SparkContext(conf)

  val model = MatrixFactorizationModel.load(sc, modelFilePath)

  val recommender = new Recommender(model)

  implicit val system = ActorSystem("recommendation-engine")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val route = get {
    path("recommend") {
      parameter('profileId, 'storyId) { (profileId: String, storyId: String) =>
        val result = recommender.recommend(profileId.toInt, storyId.toInt)
        complete((StatusCodes.OK, result))
      }
    }
  }

  Http().bindAndHandle(route, "localhost", 8081)

}
