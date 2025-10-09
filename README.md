# zio-nats

[![CI](https://github.com/zio/zio-nats/actions/workflows/ci.yml/badge.svg)](https://github.com/zio/zio-nats/actions/workflows/ci.yml)
[![Release](https://github.com/zio/zio-nats/actions/workflows/release.yml/badge.svg)](https://github.com/zio/zio-nats/actions/workflows/release.yml)

A lightweight ZIO 2.x wrapper around the official Java NATS client (jnats).

## Project Status

**Version**: 0.1.0
**Status**: Ready for production use
**Compatibility**: ZIO 2.x, Scala 3.3.6+

Features:
- ZLayer-based managed connection acquisition & clean shutdown via `Nats.live` / `Nats.liveZIO`
- Simple publish & request APIs (string or byte array)
- Streaming consume API via ZStream using a NATS Dispatcher bridged through a ZIO Queue
- Minimal trait (`Nats`) you pull from the environment with `ZIO.service[Nats]`
- Configurable via ZIO Config (env vars or system properties by default)

## Usage

Provide the layer and obtain the `Nats` service:

```scala
import zio.*
import zio.nats.*

val program = for {
  nats <- ZIO.service[Nats]
  _    <- nats.publishString("demo.events", "hello world")
  msg  <- nats
            .subscribe("demo.events")
            .take(1)
            .runHead
            .someOrFail(new RuntimeException("No message received"))
  _    <- Console.printLine(s"Received: ${msg.asString()}")
} yield ()

val run = program.provide(Nats.live(NatsConfig.default), Scope.default)
```

### Requests

```scala
for {
  nats <- ZIO.service[Nats]
  response <- nats.requestString("service.echo", "ping", 2.seconds)
} yield response
```

(Requires some service already replying on that subject.)

### Raw byte APIs

If you want lower-level control:
```scala
val publishBytes: RIO[Nats, Unit] =
  ZIO.serviceWithZIO[Nats](_.publish("binary.subject", Array[Byte](1,2,3)))
```

### Layer Variants

You can derive the layer from config (env / system properties) directly:
```scala
val layer: ZLayer[Scope, Throwable, Nats] = Nats.live
```
Or provide an explicit `NatsConfig`:
```scala
val layerExplicit = Nats.live(NatsConfig(host = "localhost", port = 4222))
```
You can also customize the `Options.Builder` from jnats if you need to. See [jnats docs](https://github.com/nats-io/nats.java) for details.
```scala
val layerCustom = Nats.live(NatsConfig.default, _.maxReconnects(5))
```

## Configuration via Environment

We use ZIO Config to load configuration (nested under `nats`). Via environment variables for example:

| Variable        | Meaning                 | Default    |
|-----------------|-------------------------|------------|
| NATS_HOST       | Hostname                | localhost  |
| NATS_PORT       | Port                    | 4222       |
| NATS_USERNAME   | Username (optional)     | -          |
| NATS_PASSWORD   | Password (optional)     | -          |
| NATS_TLS        | Enable TLS (true/false) | false      |

```scala
val layer = Nats.live 
```

## Streaming Example

```scala
import zio.*
import zio.stream.*
import zio.nats.*

val streamProgram = ZStream.serviceWithStream[Nats](_.subscribe("orders.created"))
  .map(_.asString())
  .tap(str => Console.printLine(s"Order event: $str"))
  .runDrain
```

## Installation

Add to your `build.mill` file:

```scala
def mvnDeps = Seq(
  mvn"dev.zio::zio-nats:0.1.0"
)
```

Or if using sbt, add to your `build.sbt`:

```scala
libraryDependencies += "dev.zio" %% "zio-nats" % "0.1.0"
```

## Testing

An integration test spec (`NatsSpec`) exercises publish / subscribe, request / reply, and queue group distribution. Run with Mill:

```bash
mill zioNats.test
```

It uses the [java nats server runner](https://github.com/nats-io/java-nats-server-runner) and requires a NATS server being installed locally.

### Running Tests in CI

The project includes GitHub Actions workflows for continuous integration and release management. Tests run automatically on every push and pull request.

## Notes & Future Work
- Add request/reply helper for automatic responders
- Add JetStream (streaming persistence) support (future)
- Add structured error model + retries/backoff utilities

## License
Apache 2.0
