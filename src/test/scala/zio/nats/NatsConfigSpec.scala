package zio.nats

import zio.test._
import zio._

object NatsConfigSpec extends ZIOSpecDefault {
  
  def spec = suite("NatsConfig")(
    test("default configuration") {
      val config = NatsConfig.default
      
      assertTrue(
        config.servers == List("nats://localhost:4222"),
        config.connectionName.isEmpty,
        config.maxReconnects == 60,
        config.reconnectWait == 2.seconds,
        config.connectionTimeout == 2.seconds,
        config.pingInterval == 2.minutes,
        config.maxPingsOut == 2,
        config.requestCleanupInterval == 5.seconds,
        config.sslContext.isEmpty,
        config.username.isEmpty,
        config.password.isEmpty,
        config.token.isEmpty,
        !config.verbose,
        !config.pedantic
      )
    },
    
    test("configuration with single server") {
      val config = NatsConfig.withServer("nats://example.com:4222")
      
      assertTrue(
        config.servers == List("nats://example.com:4222")
      )
    },
    
    test("configuration with multiple servers") {
      val config = NatsConfig.withServers(
        "nats://server1:4222",
        "nats://server2:4222",
        "nats://server3:4222"
      )
      
      assertTrue(
        config.servers == List(
          "nats://server1:4222",
          "nats://server2:4222", 
          "nats://server3:4222"
        )
      )
    },
    
    test("configuration with authentication") {
      val config = NatsConfig.withAuth("user", "password")
      
      assertTrue(
        config.username.contains("user"),
        config.password.contains("password")
      )
    },
    
    test("configuration with token") {
      val config = NatsConfig.withToken("secret-token")
      
      assertTrue(
        config.token.contains("secret-token")
      )
    },
    
    test("convert to Java options") {
      for {
        options <- NatsConfig.default.toJavaOptions
      } yield assertTrue(
        options != null,
        options.getServers.size() == 1,
        options.getServers.get(0).toString == "nats://localhost:4222",
        options.getMaxReconnect == 60,
        options.getReconnectWait.toMillis == 2000,
        options.getConnectionTimeout.toMillis == 2000
      )
    }
  )
}
