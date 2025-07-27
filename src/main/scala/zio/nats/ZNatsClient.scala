package zio.nats

import io.nats.client.{Connection, Nats, Subscription => JSubscription}
import zio._
import zio.stream._
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * ZIO-based NATS client that wraps the Java NATS client
 */
trait ZNatsClient {
  
  /**
   * Publish a message to a subject
   */
  def publish(subject: String, data: Array[Byte]): IO[NatsError, Unit]
  
  /**
   * Publish a string message to a subject
   */
  def publish(subject: String, data: String): IO[NatsError, Unit]
  
  /**
   * Publish a message with reply subject
   */
  def publish(subject: String, replyTo: String, data: Array[Byte]): IO[NatsError, Unit]
  
  /**
   * Publish a string message with reply subject
   */
  def publish(subject: String, replyTo: String, data: String): IO[NatsError, Unit]
  
  /**
   * Subscribe to a subject and return a stream of messages
   */
  def subscribe(subject: String): IO[NatsError, MessageStream]
  
  /**
   * Subscribe to a subject with a queue group
   */
  def subscribe(subject: String, queueGroup: String): IO[NatsError, MessageStream]
  
  /**
   * Make a request and wait for a reply
   */
  def request(subject: String, data: Array[Byte], timeout: Duration): IO[NatsError, Message]
  
  /**
   * Make a string request and wait for a reply
   */
  def request(subject: String, data: String, timeout: Duration): IO[NatsError, Message]
  
  /**
   * Flush any pending messages
   */
  def flush(timeout: Duration): IO[NatsError, Unit]
  
  /**
   * Get connection statistics
   */
  def getStats: UIO[ConnectionStats]
  
  /**
   * Check if the connection is connected
   */
  def isConnected: UIO[Boolean]
  
  /**
   * Close the connection
   */
  def close: IO[NatsError, Unit]
}

object ZNatsClient {
  
  /**
   * Create a ZLayer for ZNatsClient with default configuration
   */
  def layer: ZLayer[Any, NatsError, ZNatsClient] = 
    layer(NatsConfig.default)
  
  /**
   * Create a ZLayer for ZNatsClient with server URL
   */
  def layer(serverUrl: String): ZLayer[Any, NatsError, ZNatsClient] = 
    layer(NatsConfig.withServer(serverUrl))
  
  /**
   * Create a ZLayer for ZNatsClient with configuration
   */
  def layer(config: NatsConfig): ZLayer[Any, NatsError, ZNatsClient] = 
    ZLayer.scoped {
      for {
        options <- config.toJavaOptions
        connection <- ZIO.attemptBlocking(Nats.connect(options))
          .mapError(NatsError.fromThrowable)
        client <- ZIO.succeed(new ZNatsClientImpl(connection))
        _ <- ZIO.addFinalizer(client.close.orDie)
      } yield client
    }
  
  /**
   * Create a ZNatsClient from an existing connection
   */
  def fromConnection(connection: Connection): ZNatsClient = 
    new ZNatsClientImpl(connection)
}

/**
 * Connection statistics
 */
final case class ConnectionStats(
  inMsgs: Long,
  outMsgs: Long,
  inBytes: Long,
  outBytes: Long,
  reconnects: Long,
  droppedCount: Long
)

/**
 * Implementation of ZNatsClient
 */
private class ZNatsClientImpl(connection: Connection) extends ZNatsClient {
  
  override def publish(subject: String, data: Array[Byte]): IO[NatsError, Unit] = 
    ZIO.attemptBlocking(connection.publish(subject, data))
      .mapError(ex => NatsError.PublishError(subject, ex.getMessage, Some(ex)))
      .unit
  
  override def publish(subject: String, data: String): IO[NatsError, Unit] = 
    publish(subject, data.getBytes(StandardCharsets.UTF_8))
  
  override def publish(subject: String, replyTo: String, data: Array[Byte]): IO[NatsError, Unit] = 
    ZIO.attemptBlocking(connection.publish(subject, replyTo, data))
      .mapError(ex => NatsError.PublishError(subject, ex.getMessage, Some(ex)))
      .unit
  
  override def publish(subject: String, replyTo: String, data: String): IO[NatsError, Unit] = 
    publish(subject, replyTo, data.getBytes(StandardCharsets.UTF_8))
  
  override def subscribe(subject: String): IO[NatsError, MessageStream] = 
    createSubscriptionStream(subject, None)
  
  override def subscribe(subject: String, queueGroup: String): IO[NatsError, MessageStream] = 
    createSubscriptionStream(subject, Some(queueGroup))
  
  private def createSubscriptionStream(subject: String, queueGroup: Option[String]): IO[NatsError, MessageStream] = {
    ZIO.succeed {
      ZStream.asyncScoped[Any, NatsError, Message] { callback =>
        for {
          subscription <- ZIO.attemptBlocking {
            queueGroup match {
              case Some(queue) => connection.subscribe(subject, queue)
              case None => connection.subscribe(subject)
            }
          }.mapError(ex => NatsError.SubscriptionError(subject, ex.getMessage, Some(ex)))

          _ <- ZIO.addFinalizer {
            ZIO.attemptBlocking(subscription.unsubscribe())
              .mapError(NatsError.fromThrowable)
              .orDie
          }

          fiber <- ZIO.attemptBlocking {
            val runnable = new Runnable {
              def run(): Unit = {
                try {
                  while (subscription.isActive && connection.getStatus == Connection.Status.CONNECTED) {
                    val javaMsg = subscription.nextMessage(java.time.Duration.ofMillis(100))
                    if (javaMsg != null) {
                      val msg = Message.fromJava(javaMsg)
                      callback(ZIO.succeed(Chunk.single(msg)))
                    }
                  }
                } catch {
                  case _: InterruptedException => // Expected when shutting down
                  case ex: Exception => callback(ZIO.fail(Some(NatsError.fromThrowable(ex))))
                }
              }
            }
            val thread = new Thread(runnable)
            thread.setDaemon(true)
            thread.start()
            thread
          }.mapError(ex => NatsError.SubscriptionError(subject, ex.getMessage, Some(ex)))

          _ <- ZIO.addFinalizer {
            ZIO.attemptBlocking(fiber.interrupt()).orDie
          }
        } yield ()
      }
    }
  }
  
  override def request(subject: String, data: Array[Byte], timeout: Duration): IO[NatsError, Message] = 
    ZIO.attemptBlocking {
      val javaTimeout = java.time.Duration.ofMillis(timeout.toMillis)
      val reply = connection.request(subject, data, javaTimeout)
      Message.fromJava(reply)
    }.mapError {
      case _: java.util.concurrent.TimeoutException => 
        NatsError.RequestTimeoutError(subject, timeout)
      case ex => 
        NatsError.OperationError("request", ex.getMessage, Some(ex))
    }
  
  override def request(subject: String, data: String, timeout: Duration): IO[NatsError, Message] = 
    request(subject, data.getBytes(StandardCharsets.UTF_8), timeout)
  
  override def flush(timeout: Duration): IO[NatsError, Unit] = 
    ZIO.attemptBlocking {
      val javaTimeout = java.time.Duration.ofMillis(timeout.toMillis)
      connection.flush(javaTimeout)
    }.mapError(NatsError.fromThrowable).unit
  
  override def getStats: UIO[ConnectionStats] = 
    ZIO.succeed {
      val stats = connection.getStatistics
      ConnectionStats(
        inMsgs = stats.getInMsgs,
        outMsgs = stats.getOutMsgs,
        inBytes = stats.getInBytes,
        outBytes = stats.getOutBytes,
        reconnects = stats.getReconnects,
        droppedCount = stats.getDroppedCount
      )
    }
  
  override def isConnected: UIO[Boolean] = 
    ZIO.succeed(connection.getStatus == Connection.Status.CONNECTED)
  
  override def close: IO[NatsError, Unit] = 
    ZIO.attemptBlocking(connection.close())
      .mapError(NatsError.fromThrowable)
      .unit
}
