package zio.nats

import io.nats.client.{AuthHandler, Message as JMessage, Nats as JNats, Options}
import zio.*
import zio.config.magnolia.*
import zio.stream.*

import java.net.{Inet4Address, InetAddress}
import java.nio.charset.{Charset, StandardCharsets}
import scala.jdk.CollectionConverters.*

final case class NatsConfig(
    host: String = "localhost",
    port: Int = 4222,
    username: Option[String] = None,
    password: Option[String] = None,
    tls: Boolean = false
):
  def url: String =
    val scheme = if tls then "tls" else "nats"
    s"$scheme://$host:$port"

object NatsConfig:
  val default: NatsConfig = NatsConfig()

  def config: Config[NatsConfig] =
    deriveConfig[NatsConfig].nested("nats")

final case class NatsMessage(
    subject: String,
    data: Array[Byte],
    replyTo: Option[String],
    headers: Map[String, String],
    raw: JMessage
):
  def asString(charset: Charset = StandardCharsets.UTF_8): String = String(data, charset)

trait Nats:
  def publish(subject: String, data: Array[Byte]): Task[Unit]
  def publish(subject: String, data: Chunk[Byte]): Task[Unit]                                             =
    publish(subject, data.toArray)
  def publishString(subject: String, data: String, charset: Charset = StandardCharsets.UTF_8): Task[Unit] =
    publish(subject, data.getBytes(charset))
  def flush(timeout: Duration = 1.second): Task[Unit]
  def drain(timeout: Duration = 1.second): Task[Unit]
  def request(subject: String, data: Array[Byte], timeout: Duration): Task[NatsMessage]
  def request(subject: String, data: Array[Byte]): Task[NatsMessage]                                      =
    request(subject, data, 5.seconds)
  def request(subject: String, data: Chunk[Byte], timeout: Duration): Task[NatsMessage]                   =
    request(subject, data.toArray, timeout)
  def request(subject: String, data: Chunk[Byte]): Task[NatsMessage]                                      =
    request(subject, data.toArray, 5.seconds)
  def requestString(
      subject: String,
      data: String,
      timeout: Duration,
      charset: Charset = StandardCharsets.UTF_8
  ): Task[String] =
    request(subject, data.getBytes(charset), timeout).map(msg => String(msg.data, charset))
  def subscribe(subject: String, queue: Option[String] = None): ZStream[Any, Throwable, NatsMessage]
  def reply(to: NatsMessage, data: Array[Byte]): Task[Unit]
  def replyString(to: NatsMessage, data: String, charset: Charset = StandardCharsets.UTF_8): Task[Unit]   =
    reply(to, data.getBytes(charset))

object Nats:

  def live: ZLayer[Scope, Throwable, Nats] =
    ZLayer(ZIO.config(NatsConfig.config).flatMap(acquire(_, None)))

  def live(additional: Options.Builder => Options.Builder): ZLayer[Scope, Throwable, Nats] =
    ZLayer(ZIO.config(NatsConfig.config).flatMap(acquire(_, Some(additional))))
  def live(config: NatsConfig): ZLayer[Scope, Throwable, Nats]                             =
    ZLayer(acquire(config, None))

  def live(config: NatsConfig, additional: Options.Builder => Options.Builder): ZLayer[Scope, Throwable, Nats] =
    ZLayer(acquire(config, Some(additional)))

  def liveZIO(config: NatsConfig): ZIO[Scope, Throwable, Nats] =
    acquire(config, None)

  def liveZIO(config: NatsConfig, additional: Options.Builder => Options.Builder): ZIO[Scope, Throwable, Nats] =
    acquire(config, Some(additional))

  def liveZIO: ZIO[Scope, Throwable, Nats] =
    ZIO.config(NatsConfig.config).flatMap(acquire(_, None))

  def liveZIO(additional: Options.Builder => Options.Builder): ZIO[Scope, Throwable, Nats] =
    ZIO.config(NatsConfig.config).flatMap(acquire(_, Some(additional)))

  private def buildOptions(config: NatsConfig, additional: Option[Options.Builder => Options.Builder]): Options =
    val base     = new Options.Builder().server(config.url)
    val withAuth = (config.username, config.password) match
      case (Some(u), Some(p)) => base.userInfo(u.toCharArray, p.toCharArray)
      case _                  => base
    additional match
      case Some(f) => f(withAuth).build()
      case None    => withAuth.build()

  private def acquire(
      config: NatsConfig,
      additional: Option[Options.Builder => Options.Builder]
  ): ZIO[Scope, Throwable, Nats] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(JNats.connect(buildOptions(config, additional)))
      )(c => ZIO.attempt(c.close()).ignore)
      .map { conn =>
        new Nats:
          override def publish(subject: String, data: Array[Byte]): Task[Unit] =
            ZIO.attemptBlocking(conn.publish(subject, data))

          override def flush(timeout: Duration): Task[Unit] =
            ZIO.attemptBlocking(conn.flush(timeout)).unit

          override def drain(timeout: Duration): Task[Unit] =
            ZIO.fromCompletableFuture(conn.drain(timeout)).unit

          override def request(subject: String, data: Array[Byte], timeout: Duration): Task[NatsMessage] =
            ZIO.attemptBlocking {
              convert(conn.request(subject, data, timeout))
            }

          override def subscribe(subject: String, queue: Option[String]): ZStream[Any, Throwable, NatsMessage] =
            ZStream.unwrapScoped {
              for
                runtime    <- ZIO.runtime[Any]
                q          <- Queue.unbounded[NatsMessage]
                dispatcher <- ZIO.attempt {
                                conn.createDispatcher((jm: JMessage) =>
                                  Unsafe.unsafe { implicit u =>
                                    runtime.unsafe.run(q.offer(convert(jm))).getOrThrowFiberFailure()
                                  }
                                )
                              }
                _          <- ZIO.attempt {
                                queue match
                                  case Some(qn) => dispatcher.subscribe(subject, qn)
                                  case None     => dispatcher.subscribe(subject)
                              }
                _          <- ZIO.attemptBlocking(conn.flush(1.second)).ignore
                release     = ZIO.attempt {
                                dispatcher.unsubscribe(subject)
                              }.ignore *> q.shutdown
              yield ZStream.fromQueue(q).ensuring(release)
            }

          override def reply(to: NatsMessage, data: Array[Byte]): Task[Unit] =
            to.replyTo match
              case Some(r) => ZIO.attemptBlocking(conn.publish(r, data))
              case None    => ZIO.fail(new IllegalArgumentException("Message has no replyTo"))
      }

  private def convert(jm: JMessage): NatsMessage =
    val headers: Map[String, String] = Option(jm.getHeaders) match
      case Some(h) =>
        h.keySet().asScala.map(k => k -> h.get(k).asScala.mkString(",")).toMap
      case None    => Map.empty
    NatsMessage(
      subject = jm.getSubject,
      data = jm.getData,
      replyTo = Option(jm.getReplyTo),
      headers = headers,
      raw = jm
    )
