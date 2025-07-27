# Getting Started with ZIO NATS

ZIO NATS is a functional Scala client for the NATS messaging system, built on top of ZIO for composable, type-safe, and resource-safe operations.

## Installation

Add the following dependency to your `build.sbt`:

```scala
libraryDependencies += "io.github.zio" %% "zio-nats" % "0.1.0-SNAPSHOT"
```

## Prerequisites

You'll need a running NATS server. The easiest way to get started is with Docker:

```bash
docker run -p 4222:4222 -ti nats:latest
```

Or install and run the NATS server directly:

```bash
# Install NATS server
go install github.com/nats-io/nats-server/v2@latest

# Run NATS server
nats-server
```

## Basic Usage

### Publishing Messages

```scala
import zio._
import zio.nats._

object Publisher extends ZIOAppDefault {
  val program = for {
    client <- ZIO.service[ZNatsClient]
    _      <- client.publish("greetings", "Hello, NATS!")
    _      <- client.flush(5.seconds)
  } yield ()

  def run = program.provide(
    ZNatsClient.layer("nats://localhost:4222")
  )
}
```

### Subscribing to Messages

```scala
import zio._
import zio.nats._

object Subscriber extends ZIOAppDefault {
  val program = for {
    client <- ZIO.service[ZNatsClient]
    stream <- client.subscribe("greetings")
    _      <- stream.foreach { message =>
      Console.printLine(s"Received: ${message.dataAsString}")
    }
  } yield ()

  def run = program.provide(
    ZNatsClient.layer("nats://localhost:4222")
  )
}
```

### Request-Reply Pattern

```scala
import zio._
import zio.nats._

object RequestReply extends ZIOAppDefault {
  val program = for {
    client <- ZIO.service[ZNatsClient]
    
    // Start a service that responds to requests
    _ <- client.subscribe("math.add").flatMap(_.foreach { message =>
      message.replyTo match {
        case Some(replySubject) =>
          // Simple addition service
          val result = message.dataAsString.split(",") match {
            case Array(a, b) => (a.toInt + b.toInt).toString
            case _ => "ERROR"
          }
          client.publish(replySubject, result)
        case None => ZIO.unit
      }
    }).fork
    
    // Wait for service to start
    _ <- ZIO.sleep(1.second)
    
    // Make a request
    response <- client.request("math.add", "5,3", 2.seconds)
    _ <- Console.printLine(s"5 + 3 = ${response.dataAsString}")
    
  } yield ()

  def run = program.provide(
    ZNatsClient.layer("nats://localhost:4222")
  )
}
```

### Queue Groups (Load Balancing)

```scala
import zio._
import zio.nats._

object QueueExample extends ZIOAppDefault {
  def worker(id: String) = for {
    client <- ZIO.service[ZNatsClient]
    stream <- client.subscribe("work.queue", "workers")
    _      <- stream.foreach { message =>
      Console.printLine(s"Worker $id processing: ${message.dataAsString}") *>
      ZIO.sleep(1.second) // Simulate work
    }
  } yield ()

  val program = for {
    client <- ZIO.service[ZNatsClient]
    
    // Start multiple workers
    _ <- ZIO.collectAllParDiscard(List("A", "B", "C").map(worker)).fork
    
    // Send work items
    _ <- ZIO.foreachDiscard(1 to 10) { i =>
      client.publish("work.queue", s"Task $i") *>
      ZIO.sleep(500.millis)
    }
    
    _ <- ZIO.sleep(15.seconds) // Let workers process
  } yield ()

  def run = program.provide(
    ZNatsClient.layer("nats://localhost:4222")
  )
}
```

## Configuration

You can customize the NATS connection with `NatsConfig`:

```scala
import zio.nats._

val config = NatsConfig(
  servers = List("nats://server1:4222", "nats://server2:4222"),
  connectionName = Some("my-app"),
  maxReconnects = 10,
  reconnectWait = 2.seconds,
  connectionTimeout = 5.seconds
)

val layer = ZNatsClient.layer(config)
```

### Authentication

```scala
// Username/password authentication
val authConfig = NatsConfig.withAuth("username", "password")

// Token authentication  
val tokenConfig = NatsConfig.withToken("my-secret-token")

// SSL/TLS
val sslConfig = NatsConfig.withSSL(mySslContext)
```

## Error Handling

ZIO NATS provides comprehensive error handling with typed errors:

```scala
import zio.nats._

val program = for {
  client <- ZIO.service[ZNatsClient]
  result <- client.request("service", "request", 1.second)
    .catchSome {
      case NatsError.RequestTimeoutError(subject, timeout) =>
        Console.printLine(s"Request to $subject timed out after $timeout") *>
        ZIO.succeed(Message.empty)
      case NatsError.ConnectionError(message, _) =>
        Console.printLine(s"Connection error: $message") *>
        ZIO.fail(new RuntimeException("Connection failed"))
    }
} yield result
```

## Next Steps

- Check out the [examples](../src/main/scala/zio/nats/examples/) for more detailed usage patterns
- Read the [API documentation](api.md) for complete reference
- See the [advanced features](advanced.md) guide for JetStream, Key-Value store, and more

## Resources

- [NATS Documentation](https://docs.nats.io/)
- [ZIO Documentation](https://zio.dev/)
- [NATS Java Client](https://github.com/nats-io/nats.java)
