package zio.nats

import zio.test._
import zio._
import java.io.IOException

object NatsErrorSpec extends ZIOSpecDefault {
  
  def spec = suite("NatsError")(
    test("ConnectionError") {
      val error = NatsError.ConnectionError("Connection failed", Some(new IOException("IO error")))
      
      assertTrue(
        error.getMessage == "Connection failed",
        error.getCause.isInstanceOf[IOException],
        error.getCause.getMessage == "IO error"
      )
    },
    
    test("PublishError") {
      val error = NatsError.PublishError("test.subject", "Publish failed")
      
      assertTrue(
        error.getMessage == "Failed to publish to subject 'test.subject': Publish failed",
        error.getCause == null
      )
    },
    
    test("SubscriptionError") {
      val error = NatsError.SubscriptionError("test.subject", "Subscription failed")
      
      assertTrue(
        error.getMessage == "Subscription error for subject 'test.subject': Subscription failed"
      )
    },
    
    test("RequestTimeoutError") {
      val error = NatsError.RequestTimeoutError("test.subject", 5.seconds)
      
      assertTrue(
        error.getMessage == "Request to subject 'test.subject' timed out after 5 s"
      )
    },
    
    test("OperationError") {
      val error = NatsError.OperationError("flush", "Operation failed")
      
      assertTrue(
        error.getMessage == "Operation 'flush' failed: Operation failed"
      )
    },
    
    test("fromThrowable - IOException") {
      val ioException = new IOException("Connection lost")
      val error = NatsError.fromThrowable(ioException)
      
      assertTrue(
        error.isInstanceOf[NatsError.ConnectionError],
        error.getMessage == "Connection lost",
        error.getCause == ioException
      )
    },
    
    test("fromThrowable - InterruptedException") {
      val interruptedException = new InterruptedException("Thread interrupted")
      val error = NatsError.fromThrowable(interruptedException)
      
      assertTrue(
        error.isInstanceOf[NatsError.OperationError],
        error.getMessage == "Operation 'interrupted' failed: Thread interrupted",
        error.getCause == interruptedException
      )
    },
    
    test("fromThrowable - generic exception") {
      val genericException = new RuntimeException("Something went wrong")
      val error = NatsError.fromThrowable(genericException)
      
      assertTrue(
        error.isInstanceOf[NatsError.OperationError],
        error.getMessage == "Operation 'unknown' failed: Something went wrong",
        error.getCause == genericException
      )
    }
  )
}
