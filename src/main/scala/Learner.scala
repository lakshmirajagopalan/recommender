import org.apache.spark.mllib.recommendation.{
  ALS,
  MatrixFactorizationModel,
  Rating
}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Learner extends App {
  val userReadHistoryFilePath =
    "src/main/resources/userReadHistory.csv"
  val modelFilePath =
    "src/main/resources/modelPath"

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Learner")

  val sc = new SparkContext(conf)
  val spark = SparkSession.builder.getOrCreate
  import spark.implicits._

  val userReadHistory = readUserReadHistory(userReadHistoryFilePath)
  val model = train(userReadHistory.map(history =>
    Rating(history.profileId.toInt, history.storyId.toInt, 1.0)))

  model.save(sc, modelFilePath)

  def train(ratings: RDD[Rating]): MatrixFactorizationModel = {
    val rank = 10
    val numIterations = 10

    ALS.train(ratings, rank, numIterations, 0.01)
  }

  def readUserReadHistory(path: String) = {
    spark.read
      .option("header", "true")
      .csv(path)
      .as[UserReadHistory]
      .rdd
  }
}
