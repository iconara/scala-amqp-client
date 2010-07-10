import scala.actors._
import scala.actors.Actor._

import iconara.amqp._


object Example1 {
  def main(args: Array[String]) {
    // Creates a direct exchange and binds a queue to it. The queue has a
    // subscriber, which will receive a message that is published. Then the
    // queue is unbound, and the connection closed.
    
    val connection = new Connection()
    val channel = connection.createChannel()
    val exchange = channel.createExchange("xc", 'direct)
    val queue = channel.createQueue("q")
    
    queue.bind(exchange, "key")
    
    // this actor will get all messages that are published on the exchange
    val subscriber = actor {
      react {
        // messages come in the form of Delivery objects. The second argument
        // is a delivery tag (a message ID), but we don't care about that now
        case Delivery(message, _) => println(message)
      }
    }
    
    queue.subscribe(subscriber)
    
    // now publish a message
    exchange.publish("key", "Hello World!")
    
    // this is a quick and dirty way of waiting 1s and then closing the
    // connection so that the application will exit
    actor {
      reactWithin(1000) {
        case _ => connection.close()
      }
    }
  }
}