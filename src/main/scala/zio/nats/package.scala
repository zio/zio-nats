package zio

import io.nats.client.{Message => NatsMessage}
import zio._

import java.nio.charset.{Charset, StandardCharsets}

package object nats {
  implicit class MessageOps(private val message: NatsMessage) extends AnyVal {
    /**
     * Gets the message data as a string using the specified charset.
     *
     * @param charset the charset to use for decoding
     * @return the message data as a string
     */
    def getDataAsString(charset: Charset = StandardCharsets.UTF_8): String = 
      new String(message.getData, charset)

    /**
     * Replies to the message with the specified data.
     *
     * @param data the reply data
     * @return an effect that completes when the reply is sent
     */
    def reply(data: Array[Byte]): Task[Unit] = 
      ZIO.attempt(message.getConnection.publish(message.getReplyTo, data))

    /**
     * Replies to the message with the specified string data using UTF-8 encoding.
     *
     * @param message the reply message
     * @return an effect that completes when the reply is sent
     */
    def reply(message: String): Task[Unit] = 
      reply(message.getBytes(StandardCharsets.UTF_8))
  }
}
