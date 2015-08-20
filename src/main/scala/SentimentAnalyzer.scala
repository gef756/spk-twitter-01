import scala.collection.mutable
import scala.io.Source

/**
 * Created by gabe on 8/19/15.
 */
class SentimentAnalyzer {
  var sentRatings: mutable.Map[String, Int] = mutable.Map()

  Source.fromURL(getClass.getResource("/AFINN-111.txt"))
    .getLines()
    .foreach(line => {
      val lineComps: Array[String] = line.split("\t")
      val word: String = lineComps(0)
      val score: Int = lineComps(1).toInt
      sentRatings(word) = score
    })

  def getWordScore(word: String): Int = {
    val res = sentRatings.getOrElse(word, 0)
    // println(f"Getting word... $word%s with result $res%d")
    res
  }

  def getSentiment(phrase: String): Double = {
    val words = phrase.split("\\s+")
    val wordScores: Array[Int] = words.map(getWordScore)
    val score: Double = wordScores.sum
    // println(f"Score: $score%f1.2")
    score
  }

}
