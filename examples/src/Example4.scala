import scala.util.Random
import scala.actors._
import scala.actors.Actor._

import iconara.amqp._


object Example4 {
  def main(args: Array[String]) {
    // This example shows how to explicitly ack messages. A message is sent and
    // delivered to two queues, but one of them doesn't ack, so when after a
    // second the channel is asked to recover, the message is resent to the
    // non-acking queue.

    val connection = new Connection()
    val channel = connection.createChannel()
    val exchange = channel.createExchange("ackTest", 'direct)
    val queue1 = channel.createQueue()
    val queue2 = channel.createQueue()

    exchange.bind("key", queue1, queue2)

    val nonAckingSubscriber = actor {
      loop {
        react {
          case d: Delivery =>
            println("Got message, will not ack: %s".format(d.body))
          case Shutdown(_) => exit()
        }
      }
    }
    
    val ackingSubscriber = actor {
      loop {
        react {
          case d: Delivery =>
            println("Got message, will ack: %s".format(d.body))
            reply(d.createAck())
          case Shutdown(_) => exit()
        }
      }
    }

    // Subscribe the actors and specify that acks are manual ("autoAck" is
    // known as "noAck" in the amqp-client library, but double negatives are
    // too hard to wrap your head around in my view -- "autoAck" is easier to 
    // understand and true and false mean the same thing as they do if it had
    // been called "noAck")
    queue1.subscribe(ackingSubscriber, autoAck = false)
    queue2.subscribe(nonAckingSubscriber, autoAck = false)

    // This message will be delivered to both queues, but it should be 
    // redelivered once, since one of the queues never ack it. Not sure how to
    // force it to do that though. It's supposed to do that "in the future", 
    // which is a bit fuzzy.
    exchange.publish("key", "Hello World!")
    
    // Wait a second and then recover (re-send non-acked messages)
    actor {
      reactWithin(1000) {
        case _ => channel.recover()
      }
    }
    
    // Shut down soon
    actor {
      reactWithin(2000) {
        case _ => connection.close()
      }
    }
  }
}