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
  def main(args: Array[String]) {

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

    // Set up Spark StreamingContext and create stream
    val filters: Seq[String] = Nil  // add filters here
    val conf: SparkConf = new SparkConf().setAppName("Twitter01")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Filter stream to just hashtags
    val hashTags: DStream[String] = stream.flatMap(status => status.getText
                                                  .split(" ")
                                                  .filter(_.startsWith("#")))


    // Collect and print
    def countKeys(stream: DStream[String], dur: Duration):
            DStream[(Int, String)] = {
      stream.map((_, 1))
        .reduceByKeyAndWindow(_ + _, dur)
        .map{case (topic, count) => (count, topic)}
        .transform(_.sortByKey(ascending = false))
    }

    val topLast60: DStream[(Int, String)] = countKeys(hashTags, Seconds(60))
    val topLast10: DStream[(Int, String)] = countKeys(hashTags, Seconds(10))


    // Print results
    def printTop10(resultList: DStream[(Int, String)], header: String): Unit = {
      resultList.foreachRDD((rdd: RDD[(Int, String)]) => {
        val topList = rdd.take(10)
        println(header.format(rdd.count()))
        topList.foreach{case (cnt, kw) =>
            println("%s (%s)".format(kw, cnt))
        }
      })
    }

    printTop10(topLast60, "Top 10 (of %s) Results from the last 60s")
    printTop10(topLast10, "Top 10 (of %s) Results from the last 10s")

    ssc.start()
    ssc.awaitTermination()
  }


}
