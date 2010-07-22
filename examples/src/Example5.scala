import scala.actors.Actor
import scala.actors.Actor._

import iconara.amqp._


object Example5 {
  def main(args: Array[String]) {
    // This examples demonstrates a topic exchange. A producer reads Twitter's
    // public timeline every 10 seconds and publishes two kinds of messages:
    // <screenName>.tweet and <screenName>.mentioned. There's one queue that
    // listens for all tweets, and another that listens for mentions. If you
    // supply a screen name as argument there will be a third queue that 
    // listens for activity for that user.
    
    val connection = new Connection()
    val channel = connection.createChannel()
    val tweetsExchange = channel.createExchange("tweets", 'topic)

    // Create the publisher, and give it its own channel since it will be
    // using it from a separate thread (since it's an actor). Channels and 
    // other AMQP objects are not meant to be used by multiple threads at the
    // same time.
    val publisher = new TimelinePublisher(connection.createChannel())
    publisher.start()
    
    // This queue will listen to all tweets
    val tweetQueue = channel.createQueue()
    tweetQueue.bind(tweetsExchange, "*.tweet")
    tweetQueue.subscribe(m => println("Tweet: %s".format(m)))
    
    // This queue will listen to all mentions
    val mentionsQueue = channel.createQueue()
    mentionsQueue.bind(tweetsExchange, "*.mentioned")
    mentionsQueue.subscribe(m => println("Mention: %s".format(m)))

    if (args.length > 0) {
      // If you specify a screen name on the command line this queue will be
      // created, and will listen to all activity, tweets and mentions, by 
      // that user
      val userQueue = channel.createQueue()
      userQueue.bind(tweetsExchange, "%s.*".format(args.head))
      userQueue.subscribe(m => println("Activity for %s: %s".format(args.head, m)))
    }
  }
}

class TimelinePublisher(channel: Channel) extends Actor {
  import java.net.URL
  import scala.xml.XML
  
  val url = new URL("http://api.twitter.com/1/statuses/public_timeline.xml")
  val mentionPattern = """(?<=@)(\w+)""".r
  val exchange = channel.createExchange("tweets", 'topic)
  
  def act() {
    read()
    
    // This works like a timer that calls read() every 10 seconds
    loop {
      reactWithin(10000) {
        case _ => read()
      }
    }
  }
  
  private def read() {
    val doc = XML.load(url.openStream())
    
    for (status <- (doc \\ "status")) {
      val screenName = (status \ "user" \ "screen_name").text
      val text = (status \ "text").text
      val mentions = mentionPattern.findAllIn(text)
      
      exchange.publish(message = "@%s: %s".format(screenName, text), routingKey = "%s.tweet".format(screenName))
      
      mentions.foreach { otherScreenName => 
        exchange.publish(message = "@%s mentioned @%s".format(screenName, otherScreenName), routingKey = "%s.mentioned".format(otherScreenName))
      }
    }
  }
}