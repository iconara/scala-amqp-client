package iconara.amqp


import java.util.{Map => JavaMap, HashMap => JavaHashMap}
import java.lang.{Object => JavaObject}

import scala.actors.Actor

import com.rabbitmq.client.{
  Channel => RMQChannel, 
  AMQP, 
  MessageProperties, 
  Consumer, 
  ShutdownSignalException, 
  Envelope,
  ConnectionFactory
}


private object Utils {
  def transformArguments(inArgs: Map[String, Any]): JavaMap[String, JavaObject] = {
    inArgs.foldLeft(new JavaHashMap[String, JavaObject]) { case (m, (key, value)) => 
      m.put(key, value.asInstanceOf[JavaObject])
      m 
    }
  }
  
  val emptyArguments = transformArguments(Map.empty)
}

import Utils._

class Connection(connectionFactory: ConnectionFactory = new ConnectionFactory()) {
  private lazy val connection = connectionFactory.newConnection()
  
  def createChannel(): Channel = new Channel(connection.createChannel())
  
  def close() = connection.close()
}

class Channel(channel: RMQChannel) {
  private val validExchangeTypes = Set('direct, 'fanout, 'topic)
  
  def createExchange(
    name: String, 
    exchangeType: Symbol = 'direct,
    durable: Boolean = false,
    autoDelete: Boolean = false,
    arguments: Map[String, Any] = Map.empty
  ): Exchange = {
    if (! (validExchangeTypes contains exchangeType)) {
      throw new IllegalArgumentException("\"%s\" is not a valid exchange type".format(exchangeType.name))
    }
    channel.exchangeDeclare(name, exchangeType.name, durable, autoDelete, transformArguments(arguments))
    new Exchange(name, channel)
  }
  
  def createQueue(
    name: String,
    durable: Boolean = false,
    exclusive: Boolean = false,
    autoDelete: Boolean = false,
    arguments: Map[String, Any] = Map.empty
  ): Queue = {
    channel.queueDeclare(name, durable, exclusive, autoDelete, transformArguments(arguments))
    new Queue(name, channel)
  }
  
  def createQueue(): Queue = {
    val name = channel.queueDeclare().getQueue()
    new Queue(name, channel)
  }
}

class Exchange(val name: String, channel: RMQChannel) {
  def publish(routingKey: String, message: String, mandatory: Boolean = false, immediate: Boolean = false) {
    channel.basicPublish(name, routingKey, mandatory, immediate, MessageProperties.TEXT_PLAIN, message.getBytes())
  }
}

class Queue(val name: String, channel: RMQChannel) {
  private var consumers: Map[Actor, ActorConsumerAdapter] = Map.empty
  
  def bind(exchange: Exchange, routingKey: String) {
    channel.queueBind(name, exchange.name, routingKey)
  }
  
  def unbind(exchange: Exchange, routingKey: String) {
    channel.queueUnbind(name, exchange.name, routingKey)
  }
  
  def subscribe(subscriber: Actor, autoAck: Boolean = true) {
    val consumerAdapter = new ActorConsumerAdapter(subscriber, this)
    channel.basicConsume(name, autoAck, consumerAdapter)
    consumers += (subscriber -> consumerAdapter)
  }
  
  def unsubscribe(subscriber: Actor) {
    if (consumers contains subscriber) {
      channel.basicCancel(consumers(subscriber).consumerTag)
    }
  }
  
  def ack(deliveryTag: Long) {
    channel.basicAck(deliveryTag, false)
  }
}

sealed abstract class AmqpMessage
case class Delivery(message: String, deliveryTag: Long) extends AmqpMessage
case class Shutdown(reason: String) extends AmqpMessage
case class Ack(deliveryTag: Long) extends AmqpMessage

class ActorConsumerAdapter(consumer: Actor, queue: Queue) extends Actor with Consumer {
  start()
  
  private var _consumerTag: String = null
  
  def consumerTag: String = _consumerTag
  
  override def handleConsumeOk(consumerTag: String) {
    _consumerTag = consumerTag
  }

  override def handleCancelOk(consumerTag: String) {
    this ! 'exit
  }

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException) {
    this ! (consumer, Shutdown(sig.getMessage()))
    this ! 'exit
  }

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]) {
    val message = Delivery(new String(body), envelope.getDeliveryTag())
    this ! (consumer, message)
  }
  
  def act() {
    loop {
      react {
        case (receiver: Actor, message: AmqpMessage) => receiver ! message
        case Ack(deliveryTag) => queue.ack(deliveryTag)
        case 'exit => exit()
      }
    }
  }
  
}
