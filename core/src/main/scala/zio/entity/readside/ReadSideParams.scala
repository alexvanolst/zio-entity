package zio.entity.readside

import zio.{IO, Tag}
import zio.entity.data.{ConsumerId, Tagging}

case class ReadSideParams[Id : Tag, Event, Reject](
  name: String,
  consumerId: ConsumerId,
  tagging: Tagging[Id],
  parallelism: Int = 30,
  logic: (Id, Event) => IO[Reject, Unit]
)
