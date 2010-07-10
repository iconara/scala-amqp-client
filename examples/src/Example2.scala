import scala.actors._
import scala.actors.Actor._

import iconara.amqp._


object Example2 {
  def main(args: Array[String]) {
    // Creates a fanout exchange and publishes 20 messages. Three queues are
    // bound to the exchange: the first has two subscribers, and each will
    // get half of the messages (the first the even and the second the odd).
    // The second queue has one subscriber, which will get all the messages.
    // The third keeps count of how many messages it's received. When the
    // counter reaches the number of messages that have been published, it
    // will respond to a message that the main code waits for, in effect it
    // works more or less like a countdown latch.
    
    val numMessages = 20
    
    val connection = new Connection()
    val channel = connection.createChannel()
    val exchange = channel.createExchange("news", 'fanout)
    val queue1 = channel.createQueue()
    val queue2 = channel.createQueue()
    val queue3 = channel.createQueue()

    // convenience method to bind many queues to the same routing key on the
    // same exchange
    exchange.bind("key", queue1, queue2, queue3)

    // this gets half the messages
    queue1.subscribe(actor { loop { react {
      case Delivery(message, _) => println("Q1 S1: %s".format(message))
      case Shutdown(_) => exit()
    }}})
    
    // this gets the other half
    queue1.subscribe(actor { loop { react {
      case Delivery(message, _) => println("Q1 S2: %s".format(message))
      case Shutdown(_) => exit()
    }}})
    
    // this gets all the messages
    queue2.subscribe(actor { loop { react {
      case Delivery(message, _) => println("Q2 S1: %s".format(message))
      case Shutdown(_) => exit()
    }}})
    
    val counter = actor {
      var n = 0
      loop {
        react {
          case Delivery(_, _) => n += 1
          case 'await if n >= numMessages => reply(); exit()
        }
      }
    }
    
    queue3.subscribe(counter)
    
    // publish some messages
    for (i <- 0 until numMessages) {
      exchange.publish("key", "Hello World %d".format(i))
    }
    
    // this blocks until the counter actor's counter reaches numMessages
    counter !? 'await
    
    connection.close()
  }
}