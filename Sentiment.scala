/**
  * Created by kgodse on 9/11/16.
  */

import java.io.FileWriter

import org.apache.calcite.model.JsonTable
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.io.Source
import scala.util.parsing.json._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object Sentiment {
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
//    // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
             val sqlcontext= new SQLContext(sc)
    //----------------------------------------------------------------------------------------
                              //For AFINN.txt
val arrayLines = new ArrayBuffer[String]()
    val ka=scala.io.Source.fromFile(args(1)).getLines.foreach(line => {

      val k = line.split("\t")
      arrayLines += k(0)
      arrayLines += k(1)
    })
    val ma = arrayLines.grouped(2).map(a => a(0) -> a(1).toInt).toMap
    val mapFinal = collection.mutable.Map(ma.toSeq: _*)

    //----------------------------------------------------------------------------------------
    //                            20 tweet
          val tweets = sc.textFile(args(0))

//          val tweets = sc.textFile("/home/kgodse/Documents/TestingScala/target/shortTwitter.txt")

          val tweetTable = sqlcontext.read.json(tweets)

          tweetTable.registerTempTable("tweetTable")
          val v = sqlcontext.sql("SELECT text FROM tweetTable").collect()

          val a =  v.map(_.mkString(""))

          val regex = "\\S*#(?:\\[[^\\]]+\\]|\\S+)|\\S*RT @(?:\\[[^\\]]+\\]|\\S+)|\\S*@(?:\\[[^\\]]+\\]|\\S+)|(?=\\s|$)|[^0-9a-zA-Z\\s]|(http|ftp|https):\\/\\/([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:\\/~+#-]*[\\w@?^=%&\\/~+#-])?".r
          val tweets20 = new ArrayBuffer[String]()
          val bw = new FileWriter("test.txt")
    val finalOut = new ArrayBuffer[String]()
          var counter = 0
          for(i <- 0 to a.length-1) {

          tweets20 += regex.replaceAllIn(a(i),"")

           var score = 0
            val singleTweet = tweets20(i).toLowerCase().split("\\s+")
            for(j <- 0 to singleTweet.length-1){
              var temp = mapFinal.getOrElse(singleTweet(j),0)

              if(temp != 0)
                {
                  score += temp
                }



            }

            counter = counter + 1
            try{


              finalOut += ("(" + counter + "," + score + ")" + "\n")

            }

          }


       val finalO= sc.parallelize(finalOut)

    finalO.saveAsTextFile("godse_kshitija_tweets_sentiment_first20.txt")
        }



}