package zio.entity.example.scheduler

import zio.Clock
import zio.Duration
import zio.entity.example.creditcard.readside.ActiveLocksTracker
import zio.entity.example.creditcard.readside.ActiveLocksTracker.{LockKey, LockValue}
import zio.entity.example.ledger.LedgerEntity.LedgerEntity
import zio.entity.example.storage.ExpiringStorage
import zio.stream.ZStream
import zio.{IO, Schedule, ZIO, ZLayer}

/** Poll a db table and release the lock invoking release lock on cards
  */
trait AuthorizationReleaser {
  def run: IO[AuthorizationReleaserError, Unit]
}

sealed trait AuthorizationReleaserError

object UnknownAuthorizationReleaserError extends AuthorizationReleaserError

object FixedPollAuthorizationReleaser {

  def make(
    interval: Duration
  ): ZLayer[ExpiringStorage[LockKey, LockValue] with LedgerEntity with Clock, Nothing, AuthorizationReleaser] =
    (for {
      clock   <- ZIO.service[Clock]
      ledger  <- ZIO.service[LedgerEntity]
      storage <- ZIO.service[ExpiringStorage[LockKey, LockValue]]
    } yield new AuthorizationReleaser {

      private def runPolling: ZStream[Any, AuthorizationReleaserError, Int] =
        (for {
          _ <- ZStream.fromSchedule(Schedule.fixed(interval))
          now <- ZStream.fromZIO(clock.instant)
          expired <- ZStream.fromZIO(storage.findExpired(now))
          _ <- ZStream.fromZIO(ZIO.foreach(expired)(el => ledger(el._1.ledgerId).releaseLock(el._1.lockId)))
        } yield expired.size).provideLayer(ZLayer.succeed(clock)).orElseFail(UnknownAuthorizationReleaserError)

      override def run: IO[AuthorizationReleaserError, Unit] = runPolling.runDrain
    }).toLayer

}

object JobRunners {

  def startJobs: ZIO[ActiveLocksTracker with AuthorizationReleaser, Nothing, Unit] = {
    for {
      locksTracker <- ZIO.service[ActiveLocksTracker]
      releaser     <- ZIO.service[AuthorizationReleaser]
      _            <- locksTracker.run.fork
      _            <- releaser.run.fork
    } yield ()
  }

}
