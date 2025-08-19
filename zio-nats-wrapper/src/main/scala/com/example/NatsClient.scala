package com.example.nats

import io.nats.client.{Connection, Nats, Dispatcher, Message}
import zio._
import zio.stream._

object NatsClient {

  // acquire + release safely using ZIO Scope
  def connect(url: String): ZIO[Scope, Throwable, Connection] =
    ZIO.fromAutoCloseable(ZIO.attempt(Nats.connect(url)))

  def publish(conn: Connection, subject: String, data: String): Task[Unit] =
    ZIO.attempt(conn.publish(subject, data.getBytes("UTF-8")))

  def subscribe(conn: Connection, subject: String): ZStream[Any, Throwable, Message] =
    ZStream.async[Any, Throwable, Message] { cb =>
      val dispatcher: Dispatcher = conn.createDispatcher { msg =>
        cb(ZIO.succeed(Chunk.single(msg)))
      }
      dispatcher.subscribe(subject)
    }
}
