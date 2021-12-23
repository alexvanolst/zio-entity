package zio.entity.core

import izumi.reflect.Tag
import zio.{ZIO, ZLayer}
import zio.entity.core.journal.EventJournal
import zio.entity.core.snapshot.{KeyValueStore, Snapshotting}
import zio.entity.data.Tagging

case class AlgebraCombinatorConfig[Key: Tag, State: Tag, Event: Tag](
  eventJournalOffsetStore: KeyValueStore[Key, Long],
  tagging: Tagging[Key],
  eventJournal: EventJournal[Key, Event],
  snapshotting: Snapshotting[Key, State]
)

object AlgebraCombinatorConfig {

  def fromStores[Key: Tag, Event: Tag, State: Tag](
    tagging: Tagging[Key]
  ): ZLayer[Stores[Key, Event, State], Nothing, AlgebraCombinatorConfig[Key, State, Event]] = {
    (for {
      stores <- ZIO.service[Stores[Key, Event, State]]
    } yield AlgebraCombinatorConfig(stores.offsetStore, tagging, stores.journalStore, stores.snapshotting)).toLayer
  }
}
