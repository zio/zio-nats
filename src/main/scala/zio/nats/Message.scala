package zio.nats

import io.nats.client.{Message => JMessage}
import zio._
import java.nio.charset.StandardCharsets
import scala.util.Try
import scala.jdk.CollectionConverters._

/**
 * Represents a NATS message with ZIO-friendly operations
 */
final case class Message(
  subject: String,
  replyTo: Option[String],
  data: Array[Byte],
  headers: Map[String, String] = Map.empty
) {
  
  /**
   * Get message data as a UTF-8 string
   */
  def dataAsString: String = new String(data, StandardCharsets.UTF_8)
  
  /**
   * Get message data as a string with specified charset
   */
  def dataAsString(charset: java.nio.charset.Charset): String = new String(data, charset)
  
  /**
   * Check if message has a reply subject
   */
  def hasReply: Boolean = replyTo.isDefined
  
  /**
   * Get the size of the message data in bytes
   */
  def size: Int = data.length
  
  /**
   * Check if the message is empty
   */
  def isEmpty: Boolean = data.isEmpty
  
  /**
   * Get a header value by key
   */
  def getHeader(key: String): Option[String] = headers.get(key)
  
  /**
   * Create a reply message with the same reply subject
   */
  def reply(data: Array[Byte]): Option[Message] = 
    replyTo.map(subject => Message(subject, None, data, Map.empty))
  
  /**
   * Create a reply message with string data
   */
  def reply(data: String): Option[Message] = 
    reply(data.getBytes(StandardCharsets.UTF_8))
}

object Message {
  
  /**
   * Create a message from subject and byte data
   */
  def apply(subject: String, data: Array[Byte]): Message = 
    Message(subject, None, data, Map.empty)
  
  /**
   * Create a message from subject and string data
   */
  def apply(subject: String, data: String): Message = 
    Message(subject, None, data.getBytes(StandardCharsets.UTF_8), Map.empty)
  
  /**
   * Create a message with reply subject
   */
  def withReply(subject: String, replyTo: String, data: Array[Byte]): Message = 
    Message(subject, Some(replyTo), data, Map.empty)
  
  /**
   * Create a message with reply subject and string data
   */
  def withReply(subject: String, replyTo: String, data: String): Message = 
    Message(subject, Some(replyTo), data.getBytes(StandardCharsets.UTF_8), Map.empty)
  
  /**
   * Convert from Java NATS message
   */
  def fromJava(jMsg: JMessage): Message = {
    val headers = Option(jMsg.getHeaders)
      .map(_.entrySet().asScala.map(entry => entry.getKey -> entry.getValue.asScala.mkString(",")).toMap)
      .getOrElse(Map.empty)
    
    Message(
      subject = jMsg.getSubject,
      replyTo = Option(jMsg.getReplyTo),
      data = Option(jMsg.getData).getOrElse(Array.empty),
      headers = headers
    )
  }
  
  /**
   * Empty message
   */
  val empty: Message = Message("", None, Array.empty, Map.empty)
}
