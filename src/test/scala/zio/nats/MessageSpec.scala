package zio.nats

import zio.test._
import java.nio.charset.StandardCharsets

object MessageSpec extends ZIOSpecDefault {
  
  def spec = suite("Message")(
    test("create message from subject and string data") {
      val message = Message("test.subject", "Hello, World!")
      
      assertTrue(
        message.subject == "test.subject",
        message.dataAsString == "Hello, World!",
        message.replyTo.isEmpty,
        message.headers.isEmpty,
        !message.hasReply,
        !message.isEmpty,
        message.size == "Hello, World!".getBytes(StandardCharsets.UTF_8).length
      )
    },
    
    test("create message from subject and byte data") {
      val data = "Binary data".getBytes(StandardCharsets.UTF_8)
      val message = Message("test.subject", data)
      
      assertTrue(
        message.subject == "test.subject",
        message.data.sameElements(data),
        message.dataAsString == "Binary data",
        message.replyTo.isEmpty
      )
    },
    
    test("create message with reply subject") {
      val message = Message.withReply("test.subject", "reply.subject", "Hello")
      
      assertTrue(
        message.subject == "test.subject",
        message.replyTo.contains("reply.subject"),
        message.dataAsString == "Hello",
        message.hasReply
      )
    },
    
    test("create reply message") {
      val original = Message.withReply("test.subject", "reply.subject", "Hello")
      val reply = original.reply("World")
      
      assertTrue(
        reply.isDefined,
        reply.get.subject == "reply.subject",
        reply.get.dataAsString == "World",
        reply.get.replyTo.isEmpty
      )
    },
    
    test("empty message") {
      val message = Message.empty
      
      assertTrue(
        message.subject == "",
        message.isEmpty,
        message.size == 0,
        message.replyTo.isEmpty,
        !message.hasReply
      )
    },
    
    test("message with headers") {
      val headers = Map("Content-Type" -> "application/json", "X-Custom" -> "value")
      val message = Message("test.subject", None, "{}".getBytes(), headers)
      
      assertTrue(
        message.getHeader("Content-Type").contains("application/json"),
        message.getHeader("X-Custom").contains("value"),
        message.getHeader("Non-Existent").isEmpty,
        message.headers.size == 2
      )
    }
  )
}
