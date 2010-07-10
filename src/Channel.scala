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
  
  def createChannel(): Channel = {
    new Channel(connection.createChannel())
  }
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
  def bind(exchange: Exchange, routingKey: String) {
    channel.queueBind(name, exchange.name, routingKey)
  }
  
  def unbind(exchange: Exchange, routingKey: String) {
    channel.queueUnbind(name, exchange.name, routingKey)
  }
  
  def subscribe(subscriber: Actor, autoAck: Boolean = true) {
    channel.basicConsume(name, autoAck, new ActorConsumerAdapter(subscriber))
  }
}

sealed class AmqpMessage
case class Delivery(message: String) extends AmqpMessage
case class Shutdown(reason: String) extends AmqpMessage

class ActorConsumerAdapter(consumer: Actor) extends Consumer {
  override def handleConsumeOk(consumerTag: String) { }

  override def handleCancelOk(consumerTag: String) { }

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException) {
    consumer ! Shutdown(sig.getMessage())
  }

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]) {
    consumer ! Delivery(new String(body))
  }
}
