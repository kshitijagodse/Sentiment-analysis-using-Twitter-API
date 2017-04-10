/**
  * Created by kgodse on 9/16/16.
  */
import java.io.FileWriter

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object TFDF {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlcontext= new SQLContext(sc)
    //----------------------------------------------------------------------------------------
    //                            20 tweet
//    val tweets = sc.textFile("/home/kgodse/Documents/TestingScala/godse_kshitija_first20.txt") //take input file
//    val tweets = sc.textFile("/home/kgodse/Documents/TestingScala/target/shortTwitter.txt")
    val tweets = sc.textFile(args(0))
    val tweetTable = sqlcontext.read.json(tweets)

    tweetTable.registerTempTable("tweetTable")
    val v = sqlcontext.sql("SELECT text FROM tweetTable").collect()      //sql query to fetch text object only

    val a = v.map(_.mkString(""))


    val regex = "\\S*#(?:\\[[^\\]]+\\]|\\S+)|\\S*RT @(?:\\[[^\\]]+\\]|\\S+)|\\S*@(?:\\[[^\\]]+\\]|\\S+)|(?=\\s|$)|[^0-9a-zA-Z\\s]|(http|ftp|https):\\/\\/([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:\\/~+#-]*[\\w@?^=%&\\/~+#-])?".r
    val tweets20 = new ArrayBuffer[String]()
    var dfList = scala.collection.mutable.Map[String, Int]()
    var singleTweet = new ArrayBuffer[String]()
    val finalOut = new ArrayBuffer[String]()
    //-----------------------for loop for removing unwanted texts------------------------------------------------------
    for (i <- 0 to a.length-1 ) {

     tweets20 += regex.replaceAllIn(a(i), "")


      singleTweet = singleTweet ++ tweets20(i).toLowerCase().split("\\s+")
   }
      println(singleTweet)
    val wordsArray = singleTweet.distinct.sorted.filter(_.nonEmpty)
    println(wordsArray)

    var temp : List[(Int,Int)] = List()
    var tempList : List[List[Int]] = List()

//    --------------------------loops to compute TFDF -----------------------------------------------------
      for (j <- 0 to wordsArray.length - 1) {
        var score = 0

        for (i <- 0 to tweets20.length - 1) {

          var list20 = tweets20(i).toLowerCase.split(" ").toList
          if (list20.contains(wordsArray(j))) {

          score += 1


            val count = list20.count(_ == wordsArray(j))

            temp ++= List((i+1, list20.count(_ == wordsArray(j))))

          }


      }

        finalOut += ("("+wordsArray(j)+ "," + score + "," + temp + ")")

      temp = Nil


    }
    val finalO= sc.parallelize(finalOut)
    finalO.saveAsTextFile("godse_kshitija_tweets_tfdf_first20.txt")

  }
}