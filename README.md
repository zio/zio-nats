# ZIO NATS

A Scala library providing a ZIO-based interface for the NATS messaging system.

## Overview

ZIO NATS bridges the gap between the Java NATS client and ZIO applications, offering a comprehensive wrapper that aligns with functional programming principles. This library transforms the imperative Java API into a fully functional interface that leverages ZIO's powerful effect system.

## Purpose

When integrating NATS messaging into ZIO applications, developers often face challenges with the Java client's imperative design. This library addresses these challenges by providing:

- **Native ZIO Integration**: All operations return ZIO effects, eliminating the need for manual conversions from Java Futures
- **Streaming Capabilities**: First-class support for message streams using ZIO Streams
- **Resource Management**: Automatic connection handling through ZIO's scope system
- **Intuitive API**: Simplified interface designed specifically for ZIO workflows

## Installation

Add the dependency to your project:

```sbt
libraryDependencies += "dev.zio" %% "zio-nats" % "<version>"
```

## Usage Example

The following example demonstrates the core functionality of the library:

```scala
import zio._
import zio.nats._

val program = for {
  // Publish a message to a subject
  _ <- NatsClient.publish("monitoring.alerts", "System status normal")
  
  // Process incoming messages as a stream
  _ <- NatsClient.subscribe("monitoring.alerts")
       .tap(msg => Console.printLine(s"Alert received: ${msg.getDataAsString()}"))
       .take(10)
       .runDrain
       
  // Make a request and await response
  response <- NatsClient.request("user.service.lookup", "user-123")
  userData = response.getDataAsString()
  _ <- Console.printLine(s"User data: $userData")
} yield ()

// Provide the NatsClient layer and run
object MonitoringService extends ZIOAppDefault {
  def run = program.provide(NatsClient.live)
}
```

## Configuration Options

The connection can be customized to meet specific requirements:

```scala
val customConfig = NatsConfig(
  servers = List("nats://primary:4222", "nats://backup:4222"),
  connectionName = Some("monitoring-service"),
  reconnectWait = Duration.fromSeconds(2),
  maxReconnects = 10,
  tlsConfig = Some(TlsConfig(
    trustStorePath = Some(Paths.get("/path/to/truststore")),
    trustStorePassword = Some("password")
  ))
)

val configuredLayer = NatsClient.live(customConfig)
```

## Additional Documentation

For more advanced usage patterns and detailed API documentation, please refer to the examples directory and test specifications. The library provides a comprehensive set of operations through the `NatsClient` interface, with convenient access methods on the companion object.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache License 2.0
