package zio.nats

import java.io.IOException

/**
 * Base trait for all NATS-related errors
 */
sealed trait NatsError extends Throwable

object NatsError {
  
  /**
   * Connection-related errors
   */
  final case class ConnectionError(message: String, cause: Option[Throwable] = None) extends NatsError {
    override def getMessage: String = message
    override def getCause: Throwable = cause.orNull
  }
  
  /**
   * Publishing-related errors
   */
  final case class PublishError(subject: String, message: String, cause: Option[Throwable] = None) extends NatsError {
    override def getMessage: String = s"Failed to publish to subject '$subject': $message"
    override def getCause: Throwable = cause.orNull
  }
  
  /**
   * Subscription-related errors
   */
  final case class SubscriptionError(subject: String, message: String, cause: Option[Throwable] = None) extends NatsError {
    override def getMessage: String = s"Subscription error for subject '$subject': $message"
    override def getCause: Throwable = cause.orNull
  }
  
  /**
   * Request-reply timeout errors
   */
  final case class RequestTimeoutError(subject: String, timeout: zio.Duration) extends NatsError {
    override def getMessage: String = s"Request to subject '$subject' timed out after ${timeout.render}"
  }
  
  /**
   * General operation errors
   */
  final case class OperationError(operation: String, message: String, cause: Option[Throwable] = None) extends NatsError {
    override def getMessage: String = s"Operation '$operation' failed: $message"
    override def getCause: Throwable = cause.orNull
  }
  
  /**
   * Convert Java exceptions to NATS errors
   */
  def fromThrowable(throwable: Throwable): NatsError = throwable match {
    case io: IOException => ConnectionError(io.getMessage, Some(io))
    case ex: InterruptedException => OperationError("interrupted", ex.getMessage, Some(ex))
    case ex => OperationError("unknown", ex.getMessage, Some(ex))
  }
}
