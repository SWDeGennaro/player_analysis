package io.swdegennaro.spark.playeranalysis

import java.util.Date

case class TweetContent(
    createdAt: Date, 
    subject: String, 
    text: String)
    //lat: Double,
    //long: Double)


