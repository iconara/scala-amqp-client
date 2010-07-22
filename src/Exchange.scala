package iconara.amqp


import com.rabbitmq.client.{
  Channel => RMQChannel, 
  MessageProperties
}


class Exchange(val name: String, channel: RMQChannel) {
  def publish(routingKey: String, message: String) {
    publish(routingKey, message, "UTF-8")
  }

  def publish(routingKey: String, message: String, encoding: String) {
    publish(routingKey, message.getBytes(encoding))
  }
  
  def publish(routingKey: String, message: Array[Byte], mandatory: Boolean = false, immediate: Boolean = false) {
    channel.basicPublish(name, routingKey, mandatory, immediate, MessageProperties.TEXT_PLAIN, message)
  }
  
  def bind(routingKey: String, queues: Queue *) {
    queues.foreach(_.bind(this, routingKey))
  }
  
  def delete() {
    channel.exchangeDelete(name)
  }
}
