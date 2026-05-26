package zio.nats

import zio.*
import zio.test.*
import zio.test.Assertion.*
import io.nats.*

object NatsSpec extends ZIOSpecDefault:
  override def spec =
    suite("NatsSpec")(
      test("publishString -> subscribe single message") {
        for
          subject <- Random.nextUUID.map(u => s"test.subject.${u.toString}")
          nats    <- ZIO.service[Nats]
          got     <- Promise.make[Nothing, String]
          fiber   <- nats
                       .subscribe(subject)
                       .take(1)
                       .foreach(msg => got.succeed(msg.asString()).unit)
                       .fork
          _      <- nats.flush()
          _      <- ZIO.sleep(1.second)
          _      <- nats.publishString(subject, "hello-nats").delay(100.millis)
          _      <- nats.flush()
          msgOpt  <- got.await.timeout(10.seconds)
          _       <- fiber.interrupt
          msg     <- ZIO.fromOption(msgOpt)
        yield assertTrue(msg == "hello-nats")
      },
      test("request/reply round trip") {
        for
          subject   <- Random.nextUUID.map(u => s"test.req.${u.toString}")
          nats      <- ZIO.service[Nats]
          responder <- nats
                         .subscribe(subject)
                         .take(1)
                         .foreach { msg =>
                           nats.replyString(msg, msg.asString() + "-ok")
                         }
                         .fork
          resp      <- nats.requestString(subject, "ping", 2.seconds).delay(100.millis)
          _         <- responder.join
        yield assertTrue(resp == "ping-ok")
      },
      test("flush completes on an active connection") {
        for
          subject <- Random.nextUUID.map(u => s"test.flush.${u.toString}")
          nats    <- ZIO.service[Nats]
          _       <- nats.publishString(subject, "hello")
          _       <- nats.flush()
        yield assertTrue(true)
      },
      test("queue group distributes messages across subscribers") {
        for
          subject <- Random.nextUUID.map(u => s"test.q.${u.toString}")
          nats    <- ZIO.service[Nats]
          ref1    <- Ref.make(0)
          ref2    <- Ref.make(0)
          sub1    <- nats.subscribe(subject, Some("workers")).tap(_ => ref1.update(_ + 1)).timeout(2.seconds).runDrain.fork
          sub2    <- nats.subscribe(subject, Some("workers")).tap(_ => ref2.update(_ + 1)).timeout(2.seconds).runDrain.fork
          _       <- ZIO.sleep(100.millis)
          total    = 12
          _       <- ZIO.foreachDiscard(1 to total)(_ => nats.publishString(subject, "x"))
          _       <- ZIO.sleep(1.second)
          _       <- sub1.join *> sub2.join
          c1      <- ref1.get
          c2      <- ref2.get
        yield assertTrue(c1 + c2 == total, c1 > 0, c2 > 0)
      },
      test("reply fails when no replyTo present") {
        for
          subject <- Random.nextUUID.map(u => s"test.noreply.${u.toString}")
          nats    <- ZIO.service[Nats]
          msgOpt  <- nats.subscribe(subject).take(1).runHead.fork.flatMap { fiber =>
                       nats.publishString(subject, "plain").delay(100.millis) *> fiber.join
                     }
          msg     <- ZIO.fromOption(msgOpt)
          fake     = msg.copy(replyTo = None) // simulate missing replyTo
          result  <- nats.replyString(fake, "data").either
        yield assertTrue(result.isLeft)
      },
      test("drain closes the underlying connection for subsequent publishes") {
        for
          subject <- Random.nextUUID.map(u => s"test.drain.${u.toString}")
          nats    <- ZIO.service[Nats]
          _       <- nats.drain()
          result  <- nats.publishString(subject, "after-drain").either
        yield assertTrue(result.isLeft)
      }
    ).provideLayerShared(
      NatsServerTest.runner >>>
        ZLayer {
          for
            runner <- ZIO.service[NatsServerRunner]
            port    = runner.getPort
            nats   <- Nats.liveZIO(NatsConfig(port = port))
          yield nats
        }
    ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

object NatsServerTest:
  val runner = ZLayer {
    val runner = new NatsServerRunner()
    ZIO.addFinalizer(ZIO.attempt(runner.shutdown()).orDie).as(runner)
  }
