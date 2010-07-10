package iconara.amqp


import org.specs.Specification
import org.specs.mock.Mockito

import org.mockito.{Matchers => is, ArgumentCaptor}
import org.mockito.Matchers._

import scala.actors.Actor
import scala.actors.Actor._

import java.util.{Map => JavaMap, HashMap => JavaHashMap}

import com.rabbitmq.client.{
  Channel => RMQChannel, 
  AMQP, 
  MessageProperties, 
  Consumer, 
  Envelope, 
  ShutdownSignalException,
  ConnectionFactory,
  Connection => RMQConnection
}


class AmqpSpec extends Specification("AMQP") with Mockito {
  val emptyArguments: JavaMap[String, java.lang.Object] = new JavaHashMap[String, java.lang.Object]
  val basicProperties: AMQP.BasicProperties = MessageProperties.TEXT_PLAIN
  val rmqChannel = mock[RMQChannel]
  val channel = new Channel(rmqChannel)
  
  "A connection" can {
    val connectionFactory = mock[ConnectionFactory]
    val rmqConnection = mock[RMQConnection]
    val connection = new Connection(connectionFactory)
    connectionFactory.newConnection() returns rmqConnection
    
    "create a channel" in {
      connection.createChannel()
      there was one(rmqConnection).createChannel()
    }
    
    "be disconnected" in {
      connection.close()
      there was one(rmqConnection).close()
    }
  }
  
  "A channel" can {
    "create an exchange" in {
      "with just a name" in {
        channel.createExchange("myExchange")
        there was one(rmqChannel).exchangeDeclare("myExchange", "direct", false, false, emptyArguments)
      }
      
      "with an explicit type" in {
        channel.createExchange("myExchange", 'fanout)
        there was one(rmqChannel).exchangeDeclare("myExchange", "fanout", false, false, emptyArguments)
      }
      
      "but with an illegal type it complains" in {
        channel.createExchange("myExchange", 'magic) must throwA[IllegalArgumentException]
      }
      
      "with a name, type and properties" in {
        channel.createExchange("myExchange", 'topic, true, true)
        there was one(rmqChannel).exchangeDeclare("myExchange", "topic", true, true, emptyArguments)
      }
      
      "with a name and properties (using named parameters)" in {
        channel.createExchange("myExchange", autoDelete = false, durable = true)
        there was one(rmqChannel).exchangeDeclare("myExchange", "direct", true, false, emptyArguments)
      }
      
      "with arguments" in {
        val javaArguments = new JavaHashMap[String, java.lang.Object]
        javaArguments.put("hello", "world")
        channel.createExchange("myExchange", arguments = Map("hello" -> "world"))
        there was one(rmqChannel).exchangeDeclare("myExchange", "direct", false, false, javaArguments)
      }
      
      "and returns an Exchange object with the right name" in {
        val exchange = channel.createExchange("myExchange")
        exchange.name must be equalTo("myExchange")
      }
    }

    "delete an exchange" in {
      channel.deleteExchange("myExchange")
      there was one(rmqChannel).exchangeDelete("myExchange")
    }
    
    "create a queue" in {
      "with just a name" in {
        channel.createQueue("myQueue")
        there was one(rmqChannel).queueDeclare("myQueue", false, false, false, emptyArguments)
      }
      
      "with a name and some properties" in {
        channel.createQueue("myQueue", true, true, true)
        there was one(rmqChannel).queueDeclare("myQueue", true, true, true, emptyArguments)
      }
      
      "with a name and some properties (using named parameters)" in {
        channel.createQueue("myQueue", durable = true, autoDelete = true)
        there was one(rmqChannel).queueDeclare("myQueue", true, false, true, emptyArguments)
      }
      
      "with arguments" in {
        val javaArguments = new JavaHashMap[String, java.lang.Object]
        javaArguments.put("hello", "world")
        channel.createQueue("myQueue", arguments = Map("hello" -> "world"))
        there was one(rmqChannel).queueDeclare("myQueue", false, false, false, javaArguments)
      }
      
      "and returns a Queue object with the right name" in {
        val queue = channel.createQueue("myQueue")
        queue.name must be equalTo("myQueue")
      }
    }
    
    "create an anonymous queue" in {
      val declareOk = mock[AMQP.Queue.DeclareOk]
      declareOk.getQueue() returns "helloWorld"
      rmqChannel.queueDeclare() returns declareOk
      val queue = channel.createQueue()
      queue.name must be equalTo("helloWorld")
    }
  }

  "An exchange" can {
    val exchange = channel.createExchange("myExchange")
    
    "publish a message" in {
      "with a key and a message" in {
        exchange.publish("key", "message")
        there was one(rmqChannel).basicPublish("myExchange", "key", false, false, basicProperties, "message".getBytes())
      }
      
      "with a key, message, mandatory and immediate" in {
        exchange.publish("key", "message", true, true)
        there was one(rmqChannel).basicPublish("myExchange", "key", true, true, basicProperties, "message".getBytes())
      }

      "with a key, message, mandatory and immediate (using named parameters)" in {
        exchange.publish("key", "message", immediate = true, mandatory = false)
        there was one(rmqChannel).basicPublish("myExchange", "key", false, true, basicProperties, "message".getBytes())
      }
    }
    
    "be deleted" in {
      exchange.delete()
      there was one(rmqChannel).exchangeDelete("myExchange")
    }
    
    "bind multiple queues at the same time" in {
      val queue1 = channel.createQueue("q1")
      val queue2 = channel.createQueue("q2")
      val queue3 = channel.createQueue("q3")
      exchange.bind("helloWorld", queue1, queue2, queue3)
      there was one(rmqChannel).queueBind("q1", "myExchange", "helloWorld")
      there was one(rmqChannel).queueBind("q2", "myExchange", "helloWorld")
      there was one(rmqChannel).queueBind("q3", "myExchange", "helloWorld")
    }
  }
  
  "A queue" can {
    val exchange = mock[Exchange]
    val queue = channel.createQueue("myQueue")
    
    "bind to an exchange" in {
      exchange.name returns "myExchange"
      queue.bind(exchange, "key")
      there was one(rmqChannel).queueBind("myQueue", "myExchange", "key")
    }
    
    "unbind from an exchange" in {
      exchange.name returns "myExchange"
      queue.unbind(exchange, "key")
      there was one(rmqChannel).queueUnbind("myQueue", "myExchange", "key")
    }
    
    "be subscribed to" in {
      "by an actor" in {
        val subscriber = mock[Actor]
        queue.subscribe(subscriber)
        there was one(rmqChannel).basicConsume(is.eq("myQueue"), is.eq(true), any[Consumer])
      }
      
      "by a function" in {
        queue.subscribe { msg => println(msg) }
        there was one(rmqChannel).basicConsume(is.eq("myQueue"), is.eq(true), any[Consumer])
      }
      
      "with manual ack" in {
        val subscriber = mock[Actor]
        queue.subscribe(subscriber, autoAck = false)
        there was one(rmqChannel).basicConsume(is.eq("myQueue"), is.eq(false), any[Consumer])
      }
    }
    
    "be unsubscribed from" in {
      val subscriber = mock[Actor]
      queue.subscribe(subscriber)
      queue.unsubscribe(subscriber)
      there was one(rmqChannel).basicCancel(anyString())
    }
  }
  
  "A queue subscriber" should {
    val queue = channel.createQueue("myQueue")
    val subscriber = new Honeypot()
    lazy val consumerAdapter = {
      val captor = ArgumentCaptor.forClass(classOf[Consumer])
      queue.subscribe(subscriber)
      there was one(rmqChannel).basicConsume(anyString(), is.eq(true), captor.capture())
      captor.getValue()
    }
    
    doBefore {
      subscriber.start()
    }
    
    doAfter {
      subscriber ! 'exit
    }
    
    "get messages" in {
      val envelope = mock[Envelope]
      envelope.getDeliveryTag() returns 1234L
      consumerAdapter.handleDelivery("1234", envelope, basicProperties, "theMessage".getBytes())
      subscriber.messages match {
        case Some(list) => list.head must be equalTo(Delivery("theMessage", 1234L))
        case None => fail("Got no message")
      }
    }
    
    "get a signal on shutdown" in {
      val exception = mock[ShutdownSignalException]
      exception.getMessage() returns "theShutdownMessage"
      consumerAdapter.handleShutdownSignal("1234", exception)
      subscriber.messages match {
        case Some(list) => list.head must be equalTo(Shutdown("theShutdownMessage"))
        case None => fail("Got no message")
      }
    }
    
    "be able to ack a delivery" in {
      val ackingSubscriber = actor {
        var gotDelivery = false
        loop {
          react {
            case delivery @ Delivery(msg, deliveryTag) =>
              reply(delivery.createAck())
              gotDelivery = true
            case 'get if gotDelivery => reply()
            case 'exit => exit()
          }
        }
      }
      val captor = ArgumentCaptor.forClass(classOf[Consumer])
      queue.subscribe(ackingSubscriber, autoAck = false)
      there was one(rmqChannel).basicConsume(anyString(), is.eq(false), captor.capture())
      val consumerAdapter = captor.getValue()
      val envelope = mock[Envelope]
      envelope.getDeliveryTag() returns 123L
      consumerAdapter.handleDelivery("1234", envelope, basicProperties, "theMessage".getBytes())
      (ackingSubscriber !? (100, 'get))
      there was one(rmqChannel).basicAck(123L, false)
      ackingSubscriber ! 'exit
    }
  }
}

class Honeypot extends Actor {
  var _messages: List[Any] = Nil
  
  def messages: Option[List[Any]] = (this !? (100, 'get)).asInstanceOf[Option[List[Any]]]
  
  def act() {
    loop {
      react {
        case 'exit => exit()
        case 'get if _messages.size > 0 => reply(_messages.reverse)
        case msg if msg != 'get => _messages ::= msg
      }
    }
  }
}
