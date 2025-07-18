package zio.nats

import io.nats.client.Options
import zio._

/**
 * Configuration for connecting to a NATS server.
 */
final case class NatsConfig(
  servers: List[String] = List("nats://localhost:4222"),
  connectionName: Option[String] = None,
  connectionTimeout: Duration = Duration.fromMillis(2000),
  reconnectBufferSize: Int = 8 * 1024 * 1024,
  maxReconnects: Int = Options.DEFAULT_MAX_RECONNECT,
  reconnectWait: Duration = Duration.fromMillis(Options.DEFAULT_RECONNECT_WAIT.toMillis),
  pedantic: Boolean = false,
  verbose: Boolean = false,
  noEcho: Boolean = false,
  noHeaders: Boolean = false,
  noNoResponders: Boolean = false,
  username: Option[String] = None,
  password: Option[String] = None,
  token: Option[String] = None,
  tlsConfig: Option[TlsConfig] = None
)

object NatsConfig {
  /**
   * Default configuration for connecting to a local NATS server.
   */
  val default: NatsConfig = NatsConfig()
}
