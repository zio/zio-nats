package zio.nats

import java.nio.file.Path

/**
 * Configuration for TLS connections to NATS.
 */
final case class TlsConfig(
  keyStorePath: Option[Path] = None,
  keyStorePassword: Option[String] = None,
  trustStorePath: Option[Path] = None,
  trustStorePassword: Option[String] = None,
  certPath: Option[Path] = None,
  keyPath: Option[Path] = None,
  keyPassword: Option[String] = None,
  rootCAPath: Option[Path] = None,
  disableVerification: Boolean = false
)
