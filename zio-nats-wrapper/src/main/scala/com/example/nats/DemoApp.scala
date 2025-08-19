package com.example.nats

import zio._
import zio.stream._

object DemoApp extends ZIOAppDefault {
  def run = {
    val subject = "updates"

    ZIO.scoped {
      NatsClient.connect("nats://localhost:4222").flatMap { conn =>
        for {
          _ <- NatsClient.publish(conn, subject, "Hello from ZIO-NATS!")
          _ <- NatsClient.subscribe(conn, subject)
            .take(1)
            .foreach(msg => Console.printLine("Received: " + new String(msg.getData)))
        } yield ()
      }
    }
  }
}
