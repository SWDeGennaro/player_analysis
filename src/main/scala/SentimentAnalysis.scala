package io.swdegennaro.spark.playeranalysis

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import java.io.StringReader

object SentimentAnalysis {
  def main(args: Array[String]) {
    val inputFile = "./data/rooney_tweets.csv"

    val conf = new SparkConf().setMaster("local").setAppName("TwitterSentiment")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)

    val result = input.map( line => {
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    })
    
    val tweets = result.map(x => {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      TweetContent(format.parse(x(0)), x(1), x(2))
    })

    tweets.take(10).foreach(tweet => println(tweet.text))
  }
}
