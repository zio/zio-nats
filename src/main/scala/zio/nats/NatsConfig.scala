package zio.nats

import io.nats.client.Options
import zio._
import java.time.{Duration => JDuration}
import javax.net.ssl.SSLContext

/**
 * Configuration for NATS connection
 */
final case class NatsConfig(
  servers: List[String] = List("nats://localhost:4222"),
  connectionName: Option[String] = None,
  maxReconnects: Int = 60,
  reconnectWait: Duration = 2.seconds,
  connectionTimeout: Duration = 2.seconds,
  pingInterval: Duration = 2.minutes,
  maxPingsOut: Int = 2,
  requestCleanupInterval: Duration = 5.seconds,
  sslContext: Option[SSLContext] = None,
  username: Option[String] = None,
  password: Option[String] = None,
  token: Option[String] = None,
  verbose: Boolean = false,
  pedantic: Boolean = false
) {
  
  /**
   * Convert to Java NATS Options
   */
  def toJavaOptions: UIO[Options] = ZIO.succeed {
    val builder = new Options.Builder()
    
    // Set servers
    servers.foreach(builder.server)
    
    // Set connection name
    connectionName.foreach(builder.connectionName)
    
    // Set reconnection settings
    builder.maxReconnects(maxReconnects)
    builder.reconnectWait(JDuration.ofMillis(reconnectWait.toMillis))
    
    // Set timeouts
    builder.connectionTimeout(JDuration.ofMillis(connectionTimeout.toMillis))
    builder.pingInterval(JDuration.ofMillis(pingInterval.toMillis))
    builder.maxPingsOut(maxPingsOut)
    builder.requestCleanupInterval(JDuration.ofMillis(requestCleanupInterval.toMillis))
    
    // Set SSL context
    sslContext.foreach(builder.sslContext)
    
    // Set authentication
    (username, password) match {
      case (Some(user), Some(pass)) => builder.userInfo(user, pass)
      case _ => ()
    }
    token.foreach(builder.token)
    
    // Set flags
    builder.verbose(verbose)
    builder.pedantic(pedantic)
    
    builder.build()
  }
}

object NatsConfig {
  
  /**
   * Default configuration
   */
  val default: NatsConfig = NatsConfig()
  
  /**
   * Create configuration with single server URL
   */
  def withServer(url: String): NatsConfig = 
    NatsConfig(servers = List(url))
  
  /**
   * Create configuration with multiple server URLs
   */
  def withServers(urls: String*): NatsConfig = 
    NatsConfig(servers = urls.toList)
  
  /**
   * Create configuration with authentication
   */
  def withAuth(username: String, password: String): NatsConfig = 
    NatsConfig(username = Some(username), password = Some(password))
  
  /**
   * Create configuration with token authentication
   */
  def withToken(token: String): NatsConfig = 
    NatsConfig(token = Some(token))
  
  /**
   * Create configuration with SSL context
   */
  def withSSL(sslContext: SSLContext): NatsConfig = 
    NatsConfig(sslContext = Some(sslContext))
}
