package zio.nats.examples

import io.nats.client.Message
import zio._
import zio.nats._
import zio.stream._

import java.nio.charset.StandardCharsets
import scala.util.Random

/**
 * A simple example demonstrating how to use the ZIO NATS client.
 */
object SimpleExample extends ZIOAppDefault {
  
  private val subject = "test.subject"
  
  private def publishMessage: ZIO[NatsClient, Throwable, Unit] = 
    for {
      _ <- Console.printLine("Publishing message...")
      message = s"Hello, NATS! ${Random.nextInt(1000)}"
      _ <- NatsClient.publish(subject, message)
      _ <- Console.printLine(s"Published: $message")
    } yield ()

  private def subscribeMessages: ZIO[NatsClient, Throwable, Unit] = 
    for {
      _ <- Console.printLine("Subscribing to messages...")
      messages <- NatsClient.subscribe(subject).take(3).runCollect
      _ <- ZIO.foreach(messages) { msg =>
        Console.printLine(s"Received: ${msg.getDataAsString()}")
      }
    } yield ()

  private def requestResponse: ZIO[NatsClient, Throwable, Unit] = {
    val responderSubject = "test.request"
    
    val responder: ZIO[NatsClient, Throwable, Fiber.Runtime[Throwable, Unit]] = 
      NatsClient.subscribe(responderSubject)
        .tap { msg =>
          Console.printLine(s"Got request: ${msg.getDataAsString()}") *>
          msg.reply(s"Response to '${msg.getDataAsString()}'")
        }
        .take(1)
        .runDrain
        .fork
        
    for {
      fiber <- responder
      _ <- Console.printLine("Sending request...")
      response <- NatsClient.request(responderSubject, "Hello, can you respond?")
      _ <- Console.printLine(s"Got response: ${response.getDataAsString()}")
      _ <- fiber.join
    } yield ()
  }
  
  private val program: ZIO[NatsClient, Throwable, Unit] = 
    for {
      _ <- Console.printLine("Starting NATS example...")
      _ <- publishMessage
      _ <- subscribeMessages
      _ <- requestResponse
      _ <- Console.printLine("NATS example completed.")
    } yield ()

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    program.provide(NatsClient.live)
}
