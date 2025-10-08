# zio-nats: A ZIO 2.x Wrapper for NATS

## Overview

`zio-nats` is a lightweight and idiomatic ZIO 2.x wrapper for the official Java NATS client (`jnats`). It provides a robust and easy-to-use interface for interacting with NATS, a high-performance, open-source messaging system. Designed with ZIO's principles of type-safety, concurrency, and resource management in mind, `zio-nats` simplifies building reactive and resilient microservices that leverage NATS for inter-service communication.

This library aims to provide a seamless integration with the ZIO ecosystem, offering managed resources, streaming capabilities via `ZStream`, and configurable client options, making it an excellent choice for Scala developers working with ZIO.

## Project Status

**Version:** `0.1.0`
**Stability:** Initial release. This version is considered stable for basic publish/subscribe and request/reply patterns. Future releases will focus on expanding features, including JetStream support and advanced error handling.

## Features

`zio-nats` offers a comprehensive set of features to streamline your NATS interactions within a ZIO application:

- **ZLayer-based Resource Management:** Seamless acquisition and clean shutdown of NATS connections using `Nats.live` and `Nats.liveZIO`, ensuring proper resource handling.
- **Simple Messaging APIs:** Intuitive APIs for publishing messages and making requests, supporting both `String` and raw `Byte` array payloads.
- **Streaming Consumption:** Leverage `ZStream` for efficient, back-pressured consumption of messages from NATS subjects, ideal for event-driven architectures.
- **Minimalistic Service Interface:** A lean `Nats` trait that can be easily pulled from the ZIO environment using `ZIO.service[Nats]`, promoting functional dependency injection.
- **Configurable via ZIO Config:** Flexible configuration options for NATS client settings, supporting environment variables, system properties, and custom `NatsConfig` instances.
- **Customizable NATS Client Options:** Advanced users can customize the underlying `jnats` `Options.Builder` for fine-grained control over client behavior.

## Getting Started

To use `zio-nats` in your project, add the following dependency to your `build.mill` (or `build.sbt` if you're not using Mill):

```scala
// build.mill
object zioNats extends ScalaModule {
  def scalaVersion = "2.13.10" // Or your preferred Scala version
  def ivyDeps = Agg(
    ivy"dev.zio::zio:2.0.0", // Or your preferred ZIO version
    ivy"io.nats:jnats:2.16.9", // Or the latest jnats version
    ivy"dev.zio::zio-config:3.0.0",
    ivy"dev.zio::zio-config-magnolia:3.0.0",
    ivy"dev.zio::zio-config-typesafe:3.0.0"
  )
}
```

## Usage Examples

### Basic Publish & Subscribe

This example demonstrates how to establish a NATS connection, publish a string message, and then subscribe to receive it.

```scala
import zio.*
import zio.nats.*

object BasicUsage extends ZIOAppDefault {
  val program = for {
    nats <- ZIO.service[Nats]
    _    <- Console.printLine("Publishing 'hello world' to demo.events")
    _    <- nats.publishString("demo.events", "hello world")
    msg  <- nats
              .subscribe("demo.events")
              .take(1)
              .runHead
              .someOrFail(new RuntimeException("No message received"))
    _    <- Console.printLine(s"Received: ${msg.asString()}")
  } yield ()

  override def run = program.provide(Nats.live(NatsConfig.default), Scope.default)
}
```

### Request/Reply Pattern

Implement a request/reply mechanism, where a service sends a request and waits for a response.

```scala
import zio.*
import zio.nats.*

object RequestReply extends ZIOAppDefault {
  val program = for {
    nats <- ZIO.service[Nats]
    _    <- Console.printLine("Sending request 'ping' to service.echo")
    response <- nats.requestString("service.echo", "ping", 2.seconds)
    _    <- Console.printLine(s"Received response: ${response.asString()}")
  } yield response

  override def run = program.provide(Nats.live(NatsConfig.default), Scope.default)
}
```

_(Note: This requires a separate NATS service to be actively replying on the `service.echo` subject.)_

### Raw Byte Array APIs

For scenarios requiring lower-level control or binary data transfer, `zio-nats` provides APIs for `Byte` arrays.

```scala
import zio.*
import zio.nats.*

object RawBytesUsage extends ZIOAppDefault {
  val publishBytes: RIO[Nats, Unit] =
    ZIO.serviceWithZIO[Nats](_.publish("binary.subject", Array[Byte](1, 2, 3)))

  val program = for {
    nats <- ZIO.service[Nats]
    _    <- Console.printLine("Publishing raw bytes to binary.subject")
    _    <- publishBytes
    msg  <- nats
              .subscribe("binary.subject")
              .take(1)
              .runHead
              .someOrFail(new RuntimeException("No binary message received"))
    _    <- Console.printLine(s"Received raw bytes: ${msg.getData.mkString(",")}")
  } yield ()

  override def run = program.provide(Nats.live(NatsConfig.default), Scope.default)
}
```

### Streaming with ZStream

Process a continuous flow of messages from a NATS subject using ZIO's powerful `ZStream` API.

```scala
import zio.*
import zio.stream.*
import zio.nats.*

object StreamingUsage extends ZIOAppDefault {
  val streamProgram = ZStream.serviceWithStream[Nats](_.subscribe("orders.created"))
    .map(_.asString())
    .tap(str => Console.printLine(s"Order event: $str"))
    .runDrain

  override def run = streamProgram.provide(Nats.live(NatsConfig.default), Scope.default)
}
```

## Configuration

`zio-nats` leverages ZIO Config for flexible and robust configuration management. The NATS client can be configured via environment variables, system properties, or by providing an explicit `NatsConfig` instance.

### Layer Variants

- **Default Configuration (from environment/system properties):**
  ```scala
  val layer: ZLayer[Scope, Throwable, Nats] = Nats.live
  ```
- **Explicit `NatsConfig`:**
  ```scala
  val layerExplicit = Nats.live(NatsConfig(host = "localhost", port = 4222))
  ```
- **Customizing `jnats` `Options.Builder`:**
  For advanced NATS client options, you can directly modify the `jnats` `Options.Builder`. Refer to the [jnats documentation](https://github.com/nats-io/nats.java) for a full list of available options.
  ```scala
  val layerCustom = Nats.live(NatsConfig.default, _.maxReconnects(5).connectionName("my-app-connection"))
  ```

### Environment Variables

The following environment variables (prefixed with `NATS_`) can be used to configure the NATS client:

| Variable        | Meaning                                     | Default     |
| :-------------- | :------------------------------------------ | :---------- |
| `NATS_HOST`     | Hostname of the NATS server                 | `localhost` |
| `NATS_PORT`     | Port of the NATS server                     | `4222`      |
| `NATS_USERNAME` | Username for NATS authentication (optional) | `-`         |
| `NATS_PASSWORD` | Password for NATS authentication (optional) | `-`         |
| `NATS_TLS`      | Enable TLS/SSL connection (true/false)      | `false`     |

## Testing

`zio-nats` includes an integration test suite (`NatsSpec`) that covers core functionalities like publish/subscribe, request/reply, and queue group distribution.

To run the tests using Mill:

```bash
mill zioNats.test
```

The integration tests utilize the [java nats server runner](https://github.com/nats-io/java-nats-server-runner) to spin up a local NATS server instance, ensuring a realistic testing environment.

## Deployment

This project uses GitHub Actions for automated deployment and release management.

### Workflow Overview

The deployment workflow (`.github/workflows/deploy.yml`) is configured to:

1.  **Checkout Code:** Retrieves the project source code.
2.  **Setup Java & Mill:** Configures the necessary Java Development Kit (JDK) and Mill build tool environment.
3.  **Build & Compile:** This step involves building and compiling the project artifacts.
4.  **Create GitHub Release:** Automatically creates a new GitHub Release when a version tag is pushed.

### Triggering a Deployment

To trigger a new deployment and create a GitHub Release:

1.  Ensure all your changes are committed and pushed to the `main` branch of your repository.
2.  Create a new Git tag following the [Semantic Versioning](https://semver.org/) `v*.*.*` pattern (e.g., `v0.1.0`, `v0.1.1`, `v1.0.0`).
    ```bash
    git tag v0.1.0
    ```
3.  Push the newly created tag to your GitHub repository:
    ```bash
    git push origin v0.1.0
    ```
    This action will automatically initiate the `Deploy` GitHub Actions workflow, resulting in a new GitHub Release being created for the specified version.

## Contributing

We welcome contributions to `zio-nats`! If you're interested in improving the library, please consider:

- Reporting bugs or suggesting features via GitHub Issues.
- Submitting pull requests for bug fixes or new features.
- Improving documentation or adding more usage examples.

Please ensure your contributions adhere to the existing code style and include appropriate tests.

## Future Work

- **Request/Reply Helpers:** Implement more sophisticated helpers for automatic responders and advanced request/reply patterns.
- **JetStream Support:** Integrate comprehensive support for NATS JetStream, enabling persistent streams, consumers, and advanced messaging paradigms.
- **Structured Error Model:** Develop a more granular and structured error model for NATS-related operations, enhancing error handling and recovery.
- **Retries & Backoff Utilities:** Provide built-in utilities for message publishing and consumption with configurable retry policies and exponential backoff.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
