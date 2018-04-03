import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._
trait Formats extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val recommendationFormat = jsonFormat2(Recommendation.apply)
}
