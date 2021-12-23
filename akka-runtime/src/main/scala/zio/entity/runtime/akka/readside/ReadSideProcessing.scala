package zio.entity.runtime.akka.readside

import akka.actor.ActorSystem
import zio.entity.readside.{KillSwitch, ReadSideProcess, ReadSideProcessing => TReadSideProcessing}
import zio.stream.ZStream
import zio.{ZIO, ZLayer}

object ReadSideProcessing {

  def start(name: String, processes: List[ReadSideProcess]): ZStream[TReadSideProcessing, Throwable, KillSwitch] =
    ZStream.environmentWithStream[TReadSideProcessing](_.get.start(name, processes))

  val live: ZLayer[ActorSystem with ReadSideSettings, Nothing, TReadSideProcessing] =
    ZLayer.fromServices[ActorSystem, ReadSideSettings, TReadSideProcessing] { (actorSystem: ActorSystem, readSideSettings: ReadSideSettings) =>
      ActorReadSideProcessing(actorSystem, readSideSettings)
    }
}
