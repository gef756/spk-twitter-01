/**
 * Created by gabe on 8/18/15.
 */

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

object Twitter01 {

  val sentAn: SentimentAnalyzer = new SentimentAnalyzer

  def registerCreds(): Unit = {
    // Set up authentication credentials
    val config = ConfigFactory.load()
    val creds = config.getConfig("creds")
    System.setProperty("twitter4j.oauth.consumerKey",
      creds.getString("twitter_api_key"))
    System.setProperty("twitter4j.oauth.consumerSecret",
      creds.getString("twitter_api_secret"))
    System.setProperty("twitter4j.oauth.accessToken",
      creds.getString("twitter_access_token"))
    System.setProperty("twitter4j.oauth.accessTokenSecret",
      creds.getString("twitter_access_secret"))
  }

  def countKeys(stream: DStream[String], dur: Duration):
  DStream[(Int, String)] = {
    stream.map((_, 1))
      .reduceByKeyAndWindow(_ + _, dur)
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(ascending = false))
  }

  def sumPair(pair1: (Double, Int), pair2: (Double, Int)): (Double, Int) = {
    (pair1._1 + pair2._1, pair1._2 + pair2._2)
  }

  def getSentiment(stream: DStream[String], dur: Duration, slide: Duration): DStream[Double] = {
    stream.map(tweet => (sentAn.getSentiment(tweet), 1))
      .reduceByWindow(sumPair, dur, slide)
      .map((sumCount: (Double, Int)) => sumCount._1 / sumCount._2)
  }

  def main(args: Array[String]) {

    registerCreds()

    // Set up Spark StreamingContext and create stream
    val conf: SparkConf = new SparkConf().setAppName("Twitter01")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    val streamFilters: Seq[String] = Nil  // add filters here
    val tweetStream = TwitterUtils.createStream(ssc, None, streamFilters)

//     Filter stream to just hashtags
    val hashTags: DStream[String] = tweetStream.flatMap(status => status.getText
                                                  .split(" ")
                                                  .filter(_.startsWith("#")))
    val tweets: DStream[String] = tweetStream.map(status => status.getText)

//     Collect and print
    val topLast60: DStream[(Int, String)] = countKeys(hashTags, Seconds(60))
    val topLast10: DStream[(Int, String)] = countKeys(hashTags, Seconds(10))
    val sentLast60: DStream[Double] = getSentiment(tweets, Seconds(60), Seconds(1))
    val sentLast10: DStream[Double] = getSentiment(tweets, Seconds(10), Seconds(1))


    // Print results
    def printTop10(resultList: DStream[(Int, String)], header: String): Unit = {
      resultList.foreachRDD((rdd: RDD[(Int, String)]) => {
        val topList = rdd.take(10)
        println("-----------------------------------------------------")
        println(header.format(rdd.count()))
        topList.foreach{case (cnt, kw) =>
            println("%s (%s)".format(kw, cnt))
        }
      })
    }

    def printSentiment(secs: Int)(rdd: RDD[Double]): Unit = {
        val sentiment: Double = rdd.first()
        println(f"Sentiment over the last $secs%d seconds: $sentiment%1.2f")
    }

    printTop10(topLast10, "Top 10 (of %s) Results from the last >10s<")
    sentLast10.foreachRDD(printSentiment(10)_)
    printTop10(topLast60, "Top 10 (of %s) Results from the last >>>60s<<<")
    sentLast60.foreachRDD(printSentiment(60)_)

    ssc.start()
    ssc.awaitTermination()
  }


}
