package iconara.amqp


import java.util.{Map => JavaMap, HashMap => JavaHashMap}
import java.lang.{Object => JavaObject}

import com.rabbitmq.client.{
  Channel => RMQChannel, 
  AMQP, 
  MessageProperties, 
  Consumer, 
  ShutdownSignalException, 
  Envelope,
  ConnectionFactory
}

import Utils.transformArguments


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

private object Utils {
  def transformArguments(inArgs: Map[String, Any]): JavaMap[String, JavaObject] = {
    inArgs.foldLeft(new JavaHashMap[String, JavaObject]) { case (m, (key, value)) => 
      m.put(key, value.asInstanceOf[JavaObject])
      m 
    }
  }
  
  val emptyArguments = transformArguments(Map.empty)
}