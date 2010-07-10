package iconara.amqp


import com.rabbitmq.client.ConnectionFactory


class Connection(connectionFactory: ConnectionFactory = new ConnectionFactory()) {
  private lazy val connection = connectionFactory.newConnection()
  
  def createChannel(): Channel = new Channel(connection.createChannel())
  
  def close() = connection.close()
}
