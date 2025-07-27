package zio.nats.examples

import zio._
import zio.nats._

/**
 * Basic example of subscribing to messages from NATS
 */
object BasicSubscriber extends ZIOAppDefault {
  
  val program = for {
    client <- ZIO.service[ZNatsClient]
    _      <- Console.printLine("Starting subscriber...")
    
    // Subscribe to messages
    stream <- client.subscribe("hello")
    
    // Process messages for 30 seconds
    _ <- stream
      .take(10) // Take only first 10 messages
      .foreach { message =>
        Console.printLine(s"Received on '${message.subject}': ${message.dataAsString}")
      }
      .timeout(30.seconds)
      .catchAll { _ =>
        Console.printLine("Subscription timed out")
      }
    
  } yield ()
  
  override def run = 
    program.provide(
      ZNatsClient.layer("nats://localhost:4222")
    )
}
