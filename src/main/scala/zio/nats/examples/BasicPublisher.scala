package zio.nats.examples

import zio._
import zio.nats._

/**
 * Basic example of publishing messages to NATS
 */
object BasicPublisher extends ZIOAppDefault {
  
  val program = for {
    client <- ZIO.service[ZNatsClient]
    _      <- Console.printLine("Publishing messages...")
    
    // Publish a simple string message
    _ <- client.publish("hello", "Hello, NATS!")
    _ <- Console.printLine("Published: Hello, NATS!")
    
    // Publish a byte array message
    _ <- client.publish("data", "Binary data".getBytes())
    _ <- Console.printLine("Published binary data")
    
    // Publish multiple messages
    _ <- ZIO.foreachDiscard(1 to 5) { i =>
      client.publish("counter", s"Message $i") *>
      Console.printLine(s"Published: Message $i")
    }
    
    // Flush to ensure all messages are sent
    _ <- client.flush(5.seconds)
    _ <- Console.printLine("All messages flushed")
    
  } yield ()
  
  override def run = 
    program.provide(
      ZNatsClient.layer("nats://localhost:4222")
    )
}
