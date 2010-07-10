package iconara.amqp


import scala.actors.Actor

import com.rabbitmq.client.{
  Channel => RMQChannel, 
  AMQP, 
  Consumer, 
  ShutdownSignalException, 
  Envelope
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
