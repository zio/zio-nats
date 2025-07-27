# ZIO NATS

A ZIO-based Scala client for [NATS](https://nats.io/) messaging system, wrapping the official Java client with functional effects.

## Features

- **Functional**: Built on ZIO for composable, type-safe, and resource-safe operations
- **Streaming**: Integration with ZIO Streams for reactive message processing
- **Resource Management**: Automatic connection lifecycle management with ZLayer
- **Error Handling**: Comprehensive error handling with ZIO's error model
- **Type Safety**: Strongly typed message publishing and subscription

## Quick Start

### Dependencies

Add to your `build.sbt`:

```scala
libraryDependencies += "io.github.zio" %% "zio-nats" % "0.1.0-SNAPSHOT"
```

### Basic Usage

```scala
import zio._
import zio.nats._

// Publishing messages
val program = for {
  client <- ZIO.service[ZNatsClient]
  _      <- client.publish("subject", "Hello, NATS!")
} yield ()

// Subscribing to messages
val subscription = for {
  client <- ZIO.service[ZNatsClient]
  stream <- client.subscribe("subject")
  _      <- stream.foreach(msg => Console.printLine(s"Received: ${msg.dataAsString}"))
} yield ()

// Running with connection layer
program.provide(ZNatsClient.layer("nats://localhost:4222"))
```

## Status

This library is in early development and implements basic NATS client operations:

- [x] Connection management
- [x] Basic publish/subscribe
- [x] Request-reply pattern
- [ ] JetStream support
- [ ] Key-Value store
- [ ] Object store

## Contributing

Contributions are welcome! Please see the [contributing guidelines](CONTRIBUTING.md) for more information.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
