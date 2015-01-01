package io.swdegennaro.spark.playeranalysis

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

object PLayerRatingJob {

  def main(args: Array[String]) {
   
    // Location of the Spark directory
    val sparkHome = "/root/spark"
    
    // URL of the Spark cluster
    val sparkUrl = "local[4]"
    
    // Location of the required JAR files
    val jarFile = "target/scala-2.10/player-analysis_2.10-0.0.1.jar"

    // HDFS directory for checkpointing
    val checkpointDir = "./checkpoint/"
    
    val sc = new StreamingContext(sparkUrl, "Player Analysis", Seconds(300), sparkHome, Seq(jarFile)) 
    
    // Configure Twitter credentials using twitter.txt
    Configuration.configureTwitterCredentials()
    
    val subject = "Rooney" //TODO: Change to arg or file
    val tweets = TwitterUtils.createStream(sc, None, Seq(subject))

    val tweetsDStream = tweets.map(status => {
      TweetContent(
        status.getCreatedAt(),
        subject, 
        status.getText())
    })

    //Save to disk
    tweetsDStream.foreach(tweetRDD => {
       tweetRDD.saveAsTextFile("tweets/" + folderTimestamp)
    })
    
    //set an HDFS for periodic checkpointing of the intermediate data
    sc.checkpoint(checkpointDir)
    sc.start()
    sc.awaitTermination()
  }
  
  // creates TS rounding down to minute
  def folderTimestamp = ((System.currentTimeMillis / 1000) / 60) * 60
  
}