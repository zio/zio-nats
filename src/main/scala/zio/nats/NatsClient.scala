package zio.nats

import io.nats.client.{Connection, JetStream, Message, MessageHandler, Subscription => JSubscription}
import zio._
import zio.stream._

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * A ZIO interface to the NATS messaging system.
 */
trait NatsClient {
  /**
   * Publishes a message to the specified subject.
   *
   * @param subject  the subject to publish to
   * @param data     the message payload
   * @return an effect that completes when the message is published
   */
  def publish(subject: String, data: Array[Byte]): Task[Unit]

  /**
   * Publishes a message to the specified subject with a reply subject.
   *
   * @param subject  the subject to publish to
   * @param replyTo  the reply subject
   * @param data     the message payload
   * @return an effect that completes when the message is published
   */
  def publish(subject: String, replyTo: String, data: Array[Byte]): Task[Unit]

  /**
   * Publishes a string message to the specified subject using UTF-8 encoding.
   *
   * @param subject  the subject to publish to
   * @param message  the string message to publish
   * @return an effect that completes when the message is published
   */
  def publish(subject: String, message: String): Task[Unit] =
    publish(subject, message.getBytes(StandardCharsets.UTF_8))

  /**
   * Publishes a string message to the specified subject with a reply subject using UTF-8 encoding.
   *
   * @param subject  the subject to publish to
   * @param replyTo  the reply subject
   * @param message  the string message to publish
   * @return an effect that completes when the message is published
   */
  def publish(subject: String, replyTo: String, message: String): Task[Unit] =
    publish(subject, replyTo, message.getBytes(StandardCharsets.UTF_8))

  /**
   * Requests a response for the specified subject.
   *
   * @param subject  the subject to send the request to
   * @param data     the request payload
   * @return an effect that completes with the response message
   */
  def request(subject: String, data: Array[Byte]): Task[Message]

  /**
   * Requests a response for the specified subject with a timeout.
   *
   * @param subject  the subject to send the request to
   * @param data     the request payload
   * @param timeout  the maximum time to wait for a response
   * @return an effect that completes with the response message, or fails if the timeout expires
   */
  def request(subject: String, data: Array[Byte], timeout: Duration): Task[Message]

  /**
   * Requests a response for the specified subject using a string payload.
   *
   * @param subject  the subject to send the request to
   * @param message  the request message
   * @return an effect that completes with the response message
   */
  def request(subject: String, message: String): Task[Message] =
    request(subject, message.getBytes(StandardCharsets.UTF_8))

  /**
   * Requests a response for the specified subject with a timeout using a string payload.
   *
   * @param subject  the subject to send the request to
   * @param message  the request message
   * @param timeout  the maximum time to wait for a response
   * @return an effect that completes with the response message, or fails if the timeout expires
   */
  def request(subject: String, message: String, timeout: Duration): Task[Message] =
    request(subject, message.getBytes(StandardCharsets.UTF_8), timeout)

  /**
   * Subscribes to a subject and returns a stream of messages.
   *
   * @param subject  the subject to subscribe to
   * @return a stream of messages
   */
  def subscribe(subject: String): Stream[Throwable, Message]

  /**
   * Subscribes to a subject with a queue group and returns a stream of messages.
   *
   * @param subject    the subject to subscribe to
   * @param queueGroup the queue group name
   * @return a stream of messages
   */
  def subscribe(subject: String, queueGroup: String): Stream[Throwable, Message]

  /**
   * Creates a JetStream context for advanced operations.
   *
   * @return a JetStream context
   */
  def jetStream: Task[JetStream]

  /**
   * Flushes all pending messages to the server.
   *
   * @return an effect that completes when all pending messages are sent
   */
  def flush: Task[Unit]

  /**
   * Gets the raw Java client connection.
   *
   * @return the underlying Java NATS connection
   */
  def connection: Connection
}

object NatsClient {
  /**
   * Creates a live NatsClient using the provided configuration.
   *
   * @param config the NATS connection configuration
   * @return a ZIO layer containing the NatsClient
   */
  def live(config: NatsConfig): ZLayer[Any, Throwable, NatsClient] =
    ZLayer.scoped {
      for {
        connection <- NatsConnection.connect(config)
        client = NatsClientLive(connection)
      } yield client
    }

  /**
   * Creates a live NatsClient using the default configuration.
   *
   * @return a ZIO layer containing the NatsClient
   */
  val live: ZLayer[Any, Throwable, NatsClient] = live(NatsConfig.default)

  /**
   * Publishes a message to the specified subject.
   *
   * @param subject  the subject to publish to
   * @param data     the message payload
   * @return an effect that completes when the message is published
   */
  def publish(subject: String, data: Array[Byte]): ZIO[NatsClient, Throwable, Unit] =
    ZIO.serviceWithZIO(_.publish(subject, data))

  /**
   * Publishes a message to the specified subject with a reply subject.
   *
   * @param subject  the subject to publish to
   * @param replyTo  the reply subject
   * @param data     the message payload
   * @return an effect that completes when the message is published
   */
  def publish(subject: String, replyTo: String, data: Array[Byte]): ZIO[NatsClient, Throwable, Unit] =
    ZIO.serviceWithZIO(_.publish(subject, replyTo, data))

  /**
   * Publishes a string message to the specified subject using UTF-8 encoding.
   *
   * @param subject  the subject to publish to
   * @param message  the string message to publish
   * @return an effect that completes when the message is published
   */
  def publish(subject: String, message: String): ZIO[NatsClient, Throwable, Unit] =
    ZIO.serviceWithZIO(_.publish(subject, message))

  /**
   * Publishes a string message to the specified subject with a reply subject using UTF-8 encoding.
   *
   * @param subject  the subject to publish to
   * @param replyTo  the reply subject
   * @param message  the string message to publish
   * @return an effect that completes when the message is published
   */
  def publish(subject: String, replyTo: String, message: String): ZIO[NatsClient, Throwable, Unit] =
    ZIO.serviceWithZIO(_.publish(subject, replyTo, message))

  /**
   * Requests a response for the specified subject.
   *
   * @param subject  the subject to send the request to
   * @param data     the request payload
   * @return an effect that completes with the response message
   */
  def request(subject: String, data: Array[Byte]): ZIO[NatsClient, Throwable, Message] =
    ZIO.serviceWithZIO(_.request(subject, data))

  /**
   * Requests a response for the specified subject with a timeout.
   *
   * @param subject  the subject to send the request to
   * @param data     the request payload
   * @param timeout  the maximum time to wait for a response
   * @return an effect that completes with the response message, or fails if the timeout expires
   */
  def request(subject: String, data: Array[Byte], timeout: Duration): ZIO[NatsClient, Throwable, Message] =
    ZIO.serviceWithZIO(_.request(subject, data, timeout))

  /**
   * Requests a response for the specified subject using a string payload.
   *
   * @param subject  the subject to send the request to
   * @param message  the request message
   * @return an effect that completes with the response message
   */
  def request(subject: String, message: String): ZIO[NatsClient, Throwable, Message] =
    ZIO.serviceWithZIO(_.request(subject, message))

  /**
   * Requests a response for the specified subject with a timeout using a string payload.
   *
   * @param subject  the subject to send the request to
   * @param message  the request message
   * @param timeout  the maximum time to wait for a response
   * @return an effect that completes with the response message, or fails if the timeout expires
   */
  def request(subject: String, message: String, timeout: Duration): ZIO[NatsClient, Throwable, Message] =
    ZIO.serviceWithZIO(_.request(subject, message, timeout))

  /**
   * Subscribes to a subject and returns a stream of messages.
   *
   * @param subject  the subject to subscribe to
   * @return a stream of messages
   */
  def subscribe(subject: String): ZStream[NatsClient, Throwable, Message] =
    ZStream.serviceWithStream(_.subscribe(subject))

  /**
   * Subscribes to a subject with a queue group and returns a stream of messages.
   *
   * @param subject    the subject to subscribe to
   * @param queueGroup the queue group name
   * @return a stream of messages
   */
  def subscribe(subject: String, queueGroup: String): ZStream[NatsClient, Throwable, Message] =
    ZStream.serviceWithStream(_.subscribe(subject, queueGroup))

  /**
   * Creates a JetStream context for advanced operations.
   *
   * @return a JetStream context
   */
  def jetStream: ZIO[NatsClient, Throwable, JetStream] =
    ZIO.serviceWithZIO(_.jetStream)

  /**
   * Flushes all pending messages to the server.
   *
   * @return an effect that completes when all pending messages are sent
   */
  def flush: ZIO[NatsClient, Throwable, Unit] =
    ZIO.serviceWithZIO(_.flush)
}

/**
 * Live implementation of the NatsClient using the Java NATS client.
 */
private case class NatsClientLive(connection: Connection) extends NatsClient {
  override def publish(subject: String, data: Array[Byte]): Task[Unit] =
    ZIO.attempt(connection.publish(subject, data))

  override def publish(subject: String, replyTo: String, data: Array[Byte]): Task[Unit] =
    ZIO.attempt(connection.publish(subject, replyTo, data))

  override def request(subject: String, data: Array[Byte]): Task[Message] =
    ZIO.fromCompletionStage(ZIO.attempt(connection.request(subject, data)))

  override def request(subject: String, data: Array[Byte], timeout: Duration): Task[Message] =
    ZIO.fromCompletionStage(ZIO.attempt(connection.request(subject, data, timeout)))

  override def subscribe(subject: String): Stream[Throwable, Message] =
    ZStream.unwrapScoped {
      for {
        queue <- Queue.unbounded[Message]
        dispatcher <- ZIO.succeed(connection.createDispatcher(new MessageHandler {
          override def onMessage(msg: Message): Unit = queue.offer(msg).unit
        }))
        subscription <- ZIO.attempt(dispatcher.subscribe(subject))
        _ <- ZIO.addFinalizer(ZIO.attempt(dispatcher.unsubscribe(subscription)).orDie)
      } yield ZStream.fromQueue(queue)
    }

  override def subscribe(subject: String, queueGroup: String): Stream[Throwable, Message] =
    ZStream.unwrapScoped {
      for {
        queue <- Queue.unbounded[Message]
        dispatcher <- ZIO.succeed(connection.createDispatcher(new MessageHandler {
          override def onMessage(msg: Message): Unit = queue.offer(msg).unit
        }))
        subscription <- ZIO.attempt(dispatcher.subscribe(subject, queueGroup))
        _ <- ZIO.addFinalizer(ZIO.attempt(dispatcher.unsubscribe(subscription)).orDie)
      } yield ZStream.fromQueue(queue)
    }

  override def jetStream: Task[JetStream] =
    ZIO.attempt(connection.jetStream())

  override def flush: Task[Unit] =
    ZIO.fromCompletionStage(ZIO.attempt(connection.flush())).unit
}
