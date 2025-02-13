package project_Scala

import java.util.Calendar

object CovidConversationsParser {

  val commaSplit = ","
  val quotes = "\""

  /** Convert from timestamp (String) to day (Int) */
  def dayFromTimestamp(timestamp: String): Int = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timestamp.trim.toLong * 1000L)
    cal.get(Calendar.DAY_OF_MONTH)
  }

  /** Function to parse reddit posts
   *
   *  @param line line that has to be parsed
   *  @return tuple containing id,subreddit.id,subreddit.name,subreddit.nsfw,created_utc,permalink,domain,url,selftext,title,score. none in case of input errors
   */
  def parseRedditPost(line: String): Option[(String, String, String, Boolean, Int, String, String, String, String, String, Int)] = {
    try {
      val input = line.split(commaSplit)
      /* this is a check for all items that do not have the proper size */
      if(input.size != 12) {
        return Some(("", "", "", false, -1, "", "", "", "", "", -1))
      }
      //since url and selftext could be non-existent, i simply added some ifs
      var url = "None"
      if(input(8).trim.nonEmpty) {
        url = input(8).trim
      }
      var selftext = "None"
      if(input(9).trim.nonEmpty) {
        selftext = input(9).trim
      }
      //since the last value could have quotes at the end, i made this simple
      // check to remove all quotes so that only the number remains
      val number = input(11).trim.replaceAll(quotes, "")
      Some((input(1).trim, input(2).trim, input(3).trim, input(4).trim.toBoolean, dayFromTimestamp(input(5)), input(6).trim, input(7).trim, url, selftext, input(10).trim , number.toInt))
    } catch {
      case _: Exception => None
    }
  }

  /** Function to parse reddit comments
   *
   *  @param line line that has to be parsed
   *  @return tuple containing id,subreddit.id,subreddit.name,subreddit.nsfw,created_utc,permalink,body,sentiment,score. none in case of input errors
   */
  def parseRedditComment(line: String): Option[(String, String, String, Boolean, Int, String, String, Double, Int)] = {
    try {
      val input = line.split(commaSplit)
      /* this is a check for all items that do not have the proper size */
      if(input.size != 10) {
        return Some(("", "", "", false, -1, "", "", -0.1, -1))
      }
      //since the last value could have quotes at the end, i made this simple
      // check to remove all quotes so that only the number remains
      val number = input(9).trim.replaceAll(quotes, "")
      Some((input(1).trim, input(2).trim, input(3).trim, input(4).trim.toBoolean, dayFromTimestamp(input(5)), input(6).trim, input(7).trim, input(8).trim.toDouble, number.toInt))
    } catch {
      case _: Exception => None
    }
  }
}