import scala.actors._
import scala.actors.Actor._

import iconara.amqp._


object Example3 {
  def main(args: Array[String]) {
    // This is more or less exactly the same as Example2, but there are two
    // queues with the same name, which shows that it's still queue name, not
    // Queue object instance that defines a queue -- and this is worth keeping
    // in mind.
    
    val numMessages = 20
    
    val connection = new Connection()
    val channel = connection.createChannel()
    val exchange = channel.createExchange("news", 'fanout)
    val queue1  = channel.createQueue("q1")
    val queue2a = channel.createQueue("q2") // these two queues have the same name, so
    val queue2b = channel.createQueue("q2") // they will act as if they were the same
    val queue3  = channel.createQueue()

    exchange.bind("key", queue1, queue2a, queue2b, queue3)

    queue1.subscribe( m => println("Q1:  %s".format(m)))
    queue2a.subscribe(m => println("Q2a: %s".format(m)))
    queue2b.subscribe(m => println("Q2b: %s".format(m)))
    
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