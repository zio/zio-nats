package zio.nats

import zio.test._
import zio._
import zio.stream._

/**
 * Integration tests for ZNatsClient
 * Note: These tests require a running NATS server on localhost:4222
 * You can start one with: nats-server --port 4222
 */
object ZNatsClientIntegrationSpec extends ZIOSpecDefault {
  
  // Test layer that provides a NATS client
  val testLayer = ZNatsClient.layer("nats://localhost:4222")
  
  def spec = suite("ZNatsClient Integration")(
    test("publish and subscribe to messages") {
      for {
        client <- ZIO.service[ZNatsClient]
        
        // Create a subscription
        stream <- client.subscribe("test.pubsub")
        
        // Collect messages in the background
        messagesPromise <- Promise.make[Nothing, List[String]]
        fiber <- stream
          .map(_.dataAsString)
          .take(3)
          .runCollect
          .map(_.toList)
          .flatMap(messagesPromise.succeed)
          .fork
        
        // Wait a bit for subscription to be ready
        _ <- ZIO.sleep(100.millis)
        
        // Publish messages
        _ <- client.publish("test.pubsub", "Message 1")
        _ <- client.publish("test.pubsub", "Message 2")
        _ <- client.publish("test.pubsub", "Message 3")
        
        // Wait for messages to be received
        messages <- messagesPromise.await.timeout(5.seconds)
        
        // Clean up
        _ <- fiber.interrupt
        
      } yield assertTrue(
        messages.isDefined,
        messages.get.contains("Message 1"),
        messages.get.contains("Message 2"),
        messages.get.contains("Message 3")
      )
    } @@ TestAspect.ignore, // Ignore by default since it requires a running NATS server
    
    test("request-reply pattern") {
      for {
        client <- ZIO.service[ZNatsClient]
        
        // Start a responder
        responderFiber <- client.subscribe("test.echo")
          .flatMap(_.foreach { message =>
            message.replyTo match {
              case Some(replySubject) =>
                client.publish(replySubject, s"Echo: ${message.dataAsString}")
              case None =>
                ZIO.unit
            }
          })
          .fork
        
        // Wait for responder to be ready
        _ <- ZIO.sleep(100.millis)
        
        // Make a request
        response <- client.request("test.echo", "Hello", 2.seconds)
        
        // Clean up
        _ <- responderFiber.interrupt
        
      } yield assertTrue(
        response.dataAsString == "Echo: Hello"
      )
    } @@ TestAspect.ignore, // Ignore by default since it requires a running NATS server
    
    test("queue group load balancing") {
      for {
        client <- ZIO.service[ZNatsClient]
        
        // Start multiple workers in the same queue group
        receivedMessages <- Ref.make(List.empty[String])
        
        worker1 <- client.subscribe("test.queue", "workers")
          .flatMap(_.foreach { message =>
            receivedMessages.update(_ :+ s"Worker1: ${message.dataAsString}")
          })
          .fork
          
        worker2 <- client.subscribe("test.queue", "workers")
          .flatMap(_.foreach { message =>
            receivedMessages.update(_ :+ s"Worker2: ${message.dataAsString}")
          })
          .fork
        
        // Wait for workers to be ready
        _ <- ZIO.sleep(100.millis)
        
        // Send messages
        _ <- ZIO.foreachDiscard(1 to 6) { i =>
          client.publish("test.queue", s"Task $i")
        }
        
        // Wait for processing
        _ <- ZIO.sleep(1.second)
        
        // Check results
        messages <- receivedMessages.get
        
        // Clean up
        _ <- worker1.interrupt
        _ <- worker2.interrupt
        
      } yield assertTrue(
        messages.length == 6,
        messages.exists(_.startsWith("Worker1:")),
        messages.exists(_.startsWith("Worker2:"))
      )
    } @@ TestAspect.ignore, // Ignore by default since it requires a running NATS server
    
    test("connection statistics") {
      for {
        client <- ZIO.service[ZNatsClient]
        
        // Get initial stats
        initialStats <- client.getStats
        
        // Publish some messages
        _ <- ZIO.foreachDiscard(1 to 5) { i =>
          client.publish("test.stats", s"Message $i")
        }
        
        // Flush to ensure messages are sent
        _ <- client.flush(1.second)
        
        // Get updated stats
        updatedStats <- client.getStats
        
      } yield assertTrue(
        updatedStats.outMsgs >= initialStats.outMsgs + 5,
        updatedStats.outBytes > initialStats.outBytes
      )
    } @@ TestAspect.ignore, // Ignore by default since it requires a running NATS server
    
    test("connection status") {
      for {
        client <- ZIO.service[ZNatsClient]
        isConnected <- client.isConnected
      } yield assertTrue(isConnected)
    } @@ TestAspect.ignore // Ignore by default since it requires a running NATS server
    
  ).provide(testLayer)
}
