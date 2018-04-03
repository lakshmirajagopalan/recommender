import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.{Map => MuMap}
import scala.util.Random
import scala.collection.JavaConversions._

object Generator extends App {
  val storyFilePath =
    "src/main/resources/storyList.csv"
  val userInterestsFilePath =
    "src/main/resources/userInterests.csv"
  val userReadHistoryFilePath =
    "src/main/resources/userReadHistory.csv"

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Generator")

  val sc = new SparkContext(conf)
  val spark = SparkSession.builder.getOrCreate
  import spark.implicits._

  val random = new Random()
  val userInterests = readUserInterests(userInterestsFilePath)
  val catalog = readStories(storyFilePath)

  writeUserReadHistory(userReadHistoryFilePath)
  def readUserInterests(path: String) = {
    spark.read
      .option("header", "true")
      .csv(path)
      .map[UserInterests] { value: Row =>
        val interests = value.getString(1).split(":").map(_.trim).toSet.toList
        UserInterests(value.getString(0), interests)
      }
      .collectAsList()
      .toList
  }

  def readStories(path: String) = {
    spark.read
      .option("header", "true")
      .csv(path)
      .map[Story] { value: Row =>
        val categories = value.getString(2).split(":").map(_.trim).toSet.toList
        Story(value.getString(0), value.getString(1), categories)
      }
      .collectAsList()
      .toList
  }

  private def getRandBetween(start: Int, end: Int): Int = {
    start + random.nextInt((end - start) + 1)
  }
  def writeUserReadHistory(path: String) = {
    val userInterestsMap = userInterests
      .map(uint => uint.profileId -> uint.interests)
      .toMap
    var interestsMap: MuMap[String, List[String]] = MuMap.empty

    catalog.foreach(entry =>
      entry.categories.foreach { cat =>
        val stories = entry.storyId :: interestsMap
          .getOrElse(cat, List.empty[String])
        interestsMap = interestsMap.updated(cat, stories)
    })

    val userReadHistory = userInterestsMap.flatMap { user =>
      val profileId = user._1
      val stories = user._2.flatMap(interest => interestsMap(interest)).toList
      val storiesCount = stories.length
      val num = getRandBetween(1, 3)

      val storiesRead = (1 to num).map { _ =>
        val storyId = getRandBetween(1, storiesCount)
        stories(storyId - 1)
      }
      storiesRead
        .map(story => UserReadHistory(profileId = profileId, storyId = story))
        .toList
    }

    val stringsToWrite = "profileId,storyId\n" ++ userReadHistory
      .map(user => user.profileId + "," + user.storyId)
      .mkString("\n")

    val pw = new PrintWriter(new File(path))
    pw.write(stringsToWrite)
    pw.close
  }
}
