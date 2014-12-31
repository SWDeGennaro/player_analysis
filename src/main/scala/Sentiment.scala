package io.swdegennaro.spark.playeranalysis

import scala.io.Source

object Sentiment {

  def positiveWords(): Set[String] = {
	Source.fromFile("/sentiment/pos_words.txt").getLines.filter(_.trim.size > 0).toSet
  }
  
  def negativeWords(): Set[String] = {
	Source.fromFile("/sentiment/neg_words.txt").getLines.filter(_.trim.size > 0).toSet
  }
  
  def stopWords(): Set[String] = {
	Source.fromFile("/sentiment/stop_words.txt").getLines.filter(_.trim.size > 0).toSet
  }

}