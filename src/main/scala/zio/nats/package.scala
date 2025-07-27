package zio

import io.nats.client.{Message => JMessage}
import zio.stream.ZStream

package object nats {
  
  /**
   * Type alias for a stream of NATS messages
   */
  type MessageStream = ZStream[Any, NatsError, Message]
  
  /**
   * Type alias for NATS subject
   */
  type Subject = String
  
  /**
   * Type alias for message data
   */
  type MessageData = Array[Byte]
  
  /**
   * Implicit conversion from Java NATS message to ZIO NATS message
   */
  implicit def javaMessageToMessage(jMsg: JMessage): Message = Message.fromJava(jMsg)
}
