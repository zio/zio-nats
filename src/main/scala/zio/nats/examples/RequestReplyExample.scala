package zio.nats.examples

import zio._
import zio.nats._

/**
 * Example of request-reply pattern with NATS
 */
object RequestReplyExample extends ZIOAppDefault {
  
  // Service that responds to requests
  val responderProgram = for {
    client <- ZIO.service[ZNatsClient]
    _      <- Console.printLine("Starting responder service...")
    
    stream <- client.subscribe("math.add")
    _ <- stream.foreach { message =>
      for {
        _ <- Console.printLine(s"Received request: ${message.dataAsString}")
        
        // Parse the request (expecting "a,b" format)
        result <- ZIO.attempt {
          val parts = message.dataAsString.split(",")
          val a = parts(0).toInt
          val b = parts(1).toInt
          (a + b).toString
        }.catchAll { _ =>
          ZIO.succeed("ERROR: Invalid format. Expected 'a,b'")
        }
        
        // Send reply if there's a reply subject
        _ <- message.replyTo match {
          case Some(replySubject) =>
            client.publish(replySubject, result) *>
            Console.printLine(s"Sent reply: $result")
          case None =>
            Console.printLine("No reply subject provided")
        }
      } yield ()
    }.fork
  } yield ()
  
  // Client that makes requests
  val requesterProgram = for {
    client <- ZIO.service[ZNatsClient]
    _      <- Console.printLine("Making requests...")
    
    // Make several requests
    _ <- ZIO.foreachDiscard(List("5,3", "10,20", "invalid", "7,13")) { request =>
      for {
        _ <- Console.printLine(s"Sending request: $request")
        response <- client.request("math.add", request, 5.seconds)
          .catchAll { error =>
            Console.printLine(s"Request failed: ${error.getMessage}") *>
            ZIO.succeed(Message("", s"ERROR: ${error.getMessage}"))
          }
        _ <- Console.printLine(s"Response: ${response.dataAsString}")
      } yield ()
    }
  } yield ()
  
  val program = for {
    // Start responder in background
    responderFiber <- responderProgram.fork
    
    // Wait a bit for responder to start
    _ <- ZIO.sleep(1.second)
    
    // Run requester
    _ <- requesterProgram
    
    // Clean up
    _ <- responderFiber.interrupt
  } yield ()
  
  override def run = 
    program.provide(
      ZNatsClient.layer("nats://localhost:4222")
    )
}
