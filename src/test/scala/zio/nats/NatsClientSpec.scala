package zio.nats

import io.nats.client.{Connection, Dispatcher, JetStream, Message}
import zio._
import zio.stream._
import zio.test._
import zio.test.Assertion._

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.{CompletableFuture => JFuture}
import scala.jdk.FutureConverters._

object NatsClientSpec extends ZIOSpecDefault {

  // Mock implementation of the NATS Connection
  class MockConnection extends Connection {
    private var closed = false
    private var messages = Map.empty[String, List[MockMessage]]
    private var dispatchers = List.empty[MockDispatcher]

    def publish(subject: String, data: Array[Byte]): Unit = {
      val msg = new MockMessage(subject, null, data, this)
      messages = messages + (subject -> (messages.getOrElse(subject, List.empty) :+ msg))
      dispatchers.foreach(_.onMessage(subject, msg))
    }

    def publish(subject: String, replyTo: String, data: Array[Byte]): Unit = {
      val msg = new MockMessage(subject, replyTo, data, this)
      messages = messages + (subject -> (messages.getOrElse(subject, List.empty) :+ msg))
      dispatchers.foreach(_.onMessage(subject, msg))
    }

    def request(subject: String, data: Array[Byte]): JFuture[Message] = {
      val replyTo = s"reply.${System.currentTimeMillis}"
      publish(subject, replyTo, data)
      val future = new JFuture[Message]()
      // Simulate reply
      val replyData = s"Reply to ${new String(data, StandardCharsets.UTF_8)}".getBytes(StandardCharsets.UTF_8)
      val replyMsg = new MockMessage(replyTo, null, replyData, this)
      future.complete(replyMsg)
      future
    }

    def request(subject: String, data: Array[Byte], timeout: Duration): JFuture[Message] = request(subject, data)

    def createDispatcher(handler: io.nats.client.MessageHandler): Dispatcher = {
      val dispatcher = new MockDispatcher(handler)
      dispatchers = dispatchers :+ dispatcher
      dispatcher
    }

    def flush(): JFuture[Void] = {
      val future = new JFuture[Void]()
      future.complete(null)
      future
    }

    def close(): Unit = { closed = true }

    def isClosed: Boolean = closed

    def jetStream(): JetStream = throw new UnsupportedOperationException("JetStream not implemented in mock")

    // Many other methods are not implemented for this mock
    def getServerInfo: io.nats.client.ServerInfo = throw new UnsupportedOperationException
    def getConnectedUrl: String = "nats://localhost:4222"
    def getServers: java.util.Collection[String] = java.util.Arrays.asList("nats://localhost:4222")
    def getOptions: io.nats.client.Options = throw new UnsupportedOperationException
    def getStatus: io.nats.client.Connection.Status = io.nats.client.Connection.Status.CONNECTED
    def getMaxPayload: Long = 1024 * 1024
    def publish(msg: Message): Unit = throw new UnsupportedOperationException
    def subscribe(subject: String): io.nats.client.Subscription = throw new UnsupportedOperationException
    def subscribe(subject: String, queueGroup: String): io.nats.client.Subscription = throw new UnsupportedOperationException
    def flush(timeout: Duration): Unit = {}
    def drain(timeout: Duration): JFuture[Boolean] = throw new UnsupportedOperationException
    def getStatistics: io.nats.client.Statistics = throw new UnsupportedOperationException
    def getErrorListeners: java.util.Collection[io.nats.client.ErrorListener] = throw new UnsupportedOperationException
    def getConnectionListeners: java.util.Collection[io.nats.client.ConnectionListener] = throw new UnsupportedOperationException
    def accountStats: io.nats.client.AccountStatistics = throw new UnsupportedOperationException
    def jetStream(options: io.nats.client.JetStreamOptions): JetStream = throw new UnsupportedOperationException
    def closeDispatcher(dispatcher: Dispatcher): Unit = {}
  }

  // Mock implementation of NATS Message
  class MockMessage(
    subject: String, 
    replyTo: String, 
    data: Array[Byte],
    connection: Connection
  ) extends Message {
    def getSubject: String = subject
    def getReplyTo: String = replyTo
    def getData: Array[Byte] = data
    def getConnection: Connection = connection

    // Other methods not needed for basic testing
    def getSID: String = ""
    def getHeaders: io.nats.client.Headers = null
    def hasHeaders: Boolean = false
    def isStatusMessage: Boolean = false
    def getStatus: io.nats.client.Status = null
  }

  // Mock implementation of Dispatcher
  class MockDispatcher(handler: io.nats.client.MessageHandler) extends Dispatcher {
    private var subscriptions = Map.empty[String, String]
    private var nextId = 0

    def subscribe(subject: String): String = {
      val id = s"sub${nextId}"
      nextId += 1
      subscriptions = subscriptions + (subject -> id)
      id
    }

    def subscribe(subject: String, queueGroup: String): String = {
      val id = s"sub${nextId}_${queueGroup}"
      nextId += 1
      subscriptions = subscriptions + (subject -> id)
      id
    }

    def onMessage(subject: String, msg: Message): Unit = {
      if (subscriptions.contains(subject)) {
        handler.onMessage(msg)
      }
    }

    // Other methods not needed for basic testing
    def unsubscribe(subscriptionId: String): Unit = {
      subscriptions = subscriptions.filter { case (_, id) => id != subscriptionId }
    }
    def unsubscribe(subscriptionId: String, maxMessages: Int): Unit = unsubscribe(subscriptionId)
    def isActive: Boolean = true
    def isDraining: Boolean = false
    def drain(timeout: Duration): JFuture[Boolean] = throw new UnsupportedOperationException
  }

  // Create a layer with a mock connection
  val mockConnectionLayer = ZLayer.succeed(new MockConnection())
  val mockClientLayer = ZLayer {
    for {
      conn <- ZIO.service[MockConnection]
    } yield NatsClientLive(conn)
  }

  override def spec = suite("NatsClientSpec")(
    test("publish should send a message") {
      for {
        _ <- NatsClient.publish("test.subject", "Hello, World!").provide(mockClientLayer, mockConnectionLayer)
      } yield assertCompletes
    },

    test("request should get a response") {
      for {
        response <- NatsClient.request("test.subject", "Request message").provide(mockClientLayer, mockConnectionLayer)
        data = new String(response.getData, StandardCharsets.UTF_8)
      } yield assert(data)(containsString("Reply to Request message"))
    },

    test("subscribe should receive messages") {
      for {
        messages <- ZIO.scoped {
          for {
            queue <- Queue.unbounded[String]
            fiber <- NatsClient.subscribe("test.subject")
              .map(msg => new String(msg.getData, StandardCharsets.UTF_8))
              .take(2)
              .tap(s => queue.offer(s))
              .runDrain
              .fork
            _ <- NatsClient.publish("test.subject", "Message 1")
            _ <- NatsClient.publish("test.subject", "Message 2")
            msg1 <- queue.take
            msg2 <- queue.take
            _ <- fiber.join
          } yield List(msg1, msg2)
        }.provide(mockClientLayer, mockConnectionLayer)
      } yield assert(messages)(equalTo(List("Message 1", "Message 2")))
    }
  )
}
