package zio.entity.test

import zio.{Clock, Duration, Tag, Task, ZEnvironment, ZIO, ZLayer}
import zio.Duration._
import zio.entity.core.Stores
import zio.entity.core.journal.{CommittableJournalStore, MemoryEventJournal, TestEventStore}
import zio.entity.core.snapshot.{KeyValueStore, MemoryKeyValueStore, Snapshotting}
import zio.entity.data.{TagConsumer, Versioned}
import zio.stream.ZStream

object TestMemoryStores {

  case class StoresForTest[Key, Event, State](
    override val snapshotting: Snapshotting[Key, State],
    override val journalStore: MemoryEventJournal[Key, Event],
    override val offsetStore: KeyValueStore[Key, Long],
    override val committableJournalStore: CommittableJournalStore[Long, Key, Event]
  ) extends Stores[Key, Event, State]
      with TestEventStore[Key, Event] {
    override def getAppendedEvent(key: Key): Task[List[Event]] = journalStore.getAppendedEvent(key)

    override def getAppendedStream(key: Key): ZStream[Any, Nothing, Event] = journalStore.getAppendedStream(key)
  }

  def make[Key: Tag, Event: Tag, State: Tag](
    polling: Duration,
    snapEvery: Int = 2
  ): ZLayer[Clock, Nothing, Stores[Key, Event, State] with TestEventStore[Key, Event]] = {

    val env = for {
        clock <- ZIO.service[Clock]
        memoryStoreM = MemoryEventJournal.make[Key, Event](polling).provideLayer(ZLayer.succeed(clock))
        journalStore        <- memoryStoreM
        snapshotStore       <- MemoryKeyValueStore.make[Key, Versioned[State]]
        offsetStore         <- MemoryKeyValueStore.make[Key, Long]
        readSideOffsetStore <- MemoryKeyValueStore.make[TagConsumer, Long]
        committableJournalStore = new CommittableJournalStore[Long, Key, Event](readSideOffsetStore, journalStore)
        stores = StoresForTest(Snapshotting.eachVersion(snapEvery, snapshotStore), journalStore, offsetStore, committableJournalStore)
      } yield ZEnvironment.apply[Stores[Key, Event, State], TestEventStore[Key, Event]](stores, stores)

    env.toLayerEnvironment
  }


}
