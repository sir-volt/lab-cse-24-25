package project_Scala

import org.apache.spark.sql.SparkSession
import utils._
import org.apache.spark.sql.SaveMode
import org.apache.spark.HashPartitioner
import org.apache.spark.sql._

object RedditCovidStudy {

    val path_to_bigDatasets = "../../../../datasets/big/"

    val path_Fullml_posts = path_to_bigDatasets + "the-reddit-covid-dataset-posts.csv"
    val path_Fullml_comments = path_to_bigDatasets + "the-reddit-covid-dataset-comments.csv"

    val path_output = "/output/AvgSentimentPercentageNSFW"

    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder.appName("RedditCovidStudy job").getOrCreate()
      val sqlContext = spark.sqlContext // needed to save as CSV
      import sqlContext.implicits._

      if(args.length < 1) {
        println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
        return
      }

      val deploymentMode = args(0)
      var writeMode = deploymentMode
      if (deploymentMode == "sharedRemote") {
        writeMode = "remote"
      }

      // Initialize input
      val rddPosts = spark.sparkContext.
        textFile(Commons.getDatasetPath(deploymentMode, path_Fullml_posts)).
        flatMap(RedditCovidParser.parseRedditPost).filter(x => x != ("", "", "", false, -1, "", "", "", "", "", -1))
      val rddComments = spark.sparkContext.
        textFile(Commons.getDatasetPath(deploymentMode, path_sample_comments)).
        flatMap(RedditCovidParser.parseRedditComment).filter(x => x != ("", "", "", false, -1, "", "", -0.1, -1))

      // Starting Job
      //PART 1: Aggregate on temporal dimension and obtain percentage of posts classified as NSFW
      //Posts are :(id,subreddit.id,subreddit.name,subreddit.nsfw,created_utc,permalink,domain,url,selftext,title,score)
      val percentageNSFWPosts = rddPosts.map(x => (x._5, (1, if (x._4) 1 else 0))) // (created_utc, (posts count, nsfw flag))
        .reduceByKey({ case ((total1, nsfw1), (total2, nsfw2)) =>
          (total1 + total2, nsfw1 + nsfw2)
        })
        .mapValues({ case (total, nsfw) =>
          //Calculate the percentage
          val percentage = (nsfw.toDouble * 100) / total
          "Percentage NSFW Posts: " + percentage + "%"
        })
      //PART 2: Aggregate on temporal dimension and obtain average sentiment in comments and percentage of comments classified as NSFW
      //Comments are: (id,subreddit.id,subreddit.name,subreddit.nsfw,created_utc,permalink,body,sentiment,score)
      val avgSentimentWithNSFWComment = rddComments.map(x => (x._5, (if (x._4) 1 else 0, 1, x._8))) //(created_utc, nsfw flag, number of posts, sentiment)
        .reduceByKey((a, b) => (
          a._1 + b._1, // Sum NSFW counters
          a._2 + b._2, // Sum total post counters
          a._3 + b._3 // Sum sentiment values
        ))
        .mapValues(reduced => {
          val (nsfwCount, totalCount, totalSentiment) = reduced
          val nsfwPercentage = (nsfwCount * 100.0) / totalCount
          val avgSentiment = totalSentiment / totalCount
          ("Percentage NSFW Comments: " + nsfwPercentage + "%", "Average Sentiment: " + avgSentiment)
        })
      // PART 3: Join on Temporal Dimension
      val finalResult = percentageNSFWPosts.join(avgSentimentWithNSFWComment).
      coalesce(1).
      toDF().write.format("csv").mode(SaveMode.Overwrite).
      save(Commons.getDatasetPath(writeMode, path_output))
    }
}