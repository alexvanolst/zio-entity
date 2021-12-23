package zio.entity.core.journal

import zio.entity.data.{EntityEvent, Tagging}
import zio.stream.Stream
import zio.{NonEmptyChunk, RIO}

/** Describes abstract event journal.
  *
  * It is expected that sequence number of the first event is 1.
  *
  * @tparam K - entity key type
  * @tparam E - event type
  */
trait EventJournal[K, E] {
  def append(key: K, offset: Long, events: NonEmptyChunk[E]): RIO[Tagging[K], Unit]
  def read(key: K, offset: Long): Stream[Throwable, EntityEvent[K, E]]
}
