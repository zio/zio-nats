package zio.nats

import io.nats.client.{Connection, Nats, Options}
import zio._

import java.nio.file.{Files, Paths}
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

/**
 * Manages the lifecycle of a NATS connection.
 */
private[nats] object NatsConnection {

  /**
   * Creates a new NATS connection with the specified configuration.
   * The connection will be automatically closed when the ZIO scope is closed.
   *
   * @param config the configuration for the connection
   * @return a scoped effect that contains the NATS connection
   */
  def connect(config: NatsConfig): ZIO[Scope, Throwable, Connection] = {
    for {
      options <- buildOptions(config)
      conn <- ZIO.acquireRelease(
        ZIO.attempt(Nats.connect(options))
      )(conn => ZIO.attempt(conn.close()).orDie)
    } yield conn
  }

  /**
   * Builds NATS connection options from the provided configuration.
   *
   * @param config the configuration for the connection
   * @return the connection options
   */
  private def buildOptions(config: NatsConfig): Task[Options] = {
    ZIO.attempt {
      val builder = new Options.Builder()
        .connectionTimeout(java.time.Duration.ofMillis(config.connectionTimeout.toMillis))
        .reconnectBufferSize(config.reconnectBufferSize)
        .maxReconnects(config.maxReconnects)
        .reconnectWait(java.time.Duration.ofMillis(config.reconnectWait.toMillis))
        .pedantic(config.pedantic)
        .verbose(config.verbose)
        .noEcho(config.noEcho)

      if (config.noHeaders) builder.noHeaders()
      if (config.noNoResponders) builder.noNoResponders()

      // Set server URLs
      builder.servers(config.servers.toArray)

      // Set connection name if provided
      config.connectionName.foreach(builder.connectionName)

      // Set authentication if provided
      (config.username, config.password) match {
        case (Some(username), Some(password)) => builder.userInfo(username, password.toCharArray)
        case _ => ()
      }

      // Set token if provided
      config.token.foreach(builder.token)

      // Configure TLS if provided
      config.tlsConfig.foreach(configureTls(builder, _))

      builder.build()
    }
  }

  /**
   * Configures TLS options for the connection.
   *
   * @param builder the options builder
   * @param tlsConfig the TLS configuration
   */
  private def configureTls(builder: Options.Builder, tlsConfig: TlsConfig): Unit = {
    if (tlsConfig.disableVerification) {
      builder.sslContext(createSSLContext(tlsConfig))
      builder.tls() // Enable TLS
      return
    }

    // Configure SSL context with certificate verification
    builder.sslContext(createSSLContext(tlsConfig))
    builder.secure()
  }

  /**
   * Creates an SSLContext from the TLS configuration.
   *
   * @param tlsConfig the TLS configuration
   * @return the SSL context
   */
  private def createSSLContext(tlsConfig: TlsConfig): SSLContext = {
    val sslContext = SSLContext.getInstance("TLSv1.2")

    // Configure key store if provided
    val keyManagers = (tlsConfig.keyStorePath, tlsConfig.keyStorePassword) match {
      case (Some(keyStorePath), Some(keyStorePassword)) =>
        val keyStore = KeyStore.getInstance("JKS")
        val keyStoreStream = Files.newInputStream(keyStorePath)
        try {
          keyStore.load(keyStoreStream, keyStorePassword.toCharArray)
        } finally {
          keyStoreStream.close()
        }
        
        val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray)
        keyManagerFactory.getKeyManagers
      case _ => null
    }

    // Configure trust store if provided
    val trustManagers = (tlsConfig.trustStorePath, tlsConfig.trustStorePassword) match {
      case (Some(trustStorePath), Some(trustStorePassword)) =>
        val trustStore = KeyStore.getInstance("JKS")
        val trustStoreStream = Files.newInputStream(trustStorePath)
        try {
          trustStore.load(trustStoreStream, trustStorePassword.toCharArray)
        } finally {
          trustStoreStream.close()
        }
        
        val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        trustManagerFactory.init(trustStore)
        trustManagerFactory.getTrustManagers
      case _ => null
    }

    sslContext.init(keyManagers, trustManagers, null)
    sslContext
  }
}
