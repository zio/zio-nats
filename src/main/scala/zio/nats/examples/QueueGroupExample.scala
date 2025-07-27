package zio.nats.examples

import zio._
import zio.nats._

/**
 * Example of using queue groups for load balancing
 */
object QueueGroupExample extends ZIOAppDefault {
  
  // Worker that processes messages from a queue group
  def worker(workerId: String) = for {
    client <- ZIO.service[ZNatsClient]
    _      <- Console.printLine(s"Worker $workerId starting...")
    
    stream <- client.subscribe("work.queue", "workers")
    _ <- stream.foreach { message =>
      for {
        _ <- Console.printLine(s"Worker $workerId processing: ${message.dataAsString}")
        
        // Simulate work
        _ <- ZIO.sleep(1.second)
        
        _ <- Console.printLine(s"Worker $workerId completed: ${message.dataAsString}")
      } yield ()
    }.fork
  } yield ()
  
  // Producer that sends work items
  val producer = for {
    client <- ZIO.service[ZNatsClient]
    _      <- Console.printLine("Producer starting...")
    
    _ <- ZIO.foreachDiscard(1 to 10) { i =>
      client.publish("work.queue", s"Task $i") *>
      Console.printLine(s"Sent: Task $i") *>
      ZIO.sleep(500.millis)
    }
  } yield ()
  
  val program = for {
    // Start multiple workers
    worker1 <- worker("A").fork
    worker2 <- worker("B").fork
    worker3 <- worker("C").fork
    
    // Wait a bit for workers to start
    _ <- ZIO.sleep(1.second)
    
    // Start producer
    _ <- producer
    
    // Let workers process for a while
    _ <- ZIO.sleep(15.seconds)
    
    // Clean up
    _ <- worker1.interrupt
    _ <- worker2.interrupt
    _ <- worker3.interrupt
  } yield ()
  
  override def run = 
    program.provide(
      ZNatsClient.layer("nats://localhost:4222")
    )
}
