package zio.entity.core.journal

import zio._
import zio.Clock
import zio.Duration
import zio.entity.core.snapshot.MemoryKeyValueStore
import zio.entity.data.{EntityEvent, EventTag, TagConsumer, Tagging}
import zio.stream.ZStream

trait TestEventStore[Key, Event] {
  def getAppendedEvent(key: Key): Task[List[Event]]
  def getAppendedStream(key: Key): ZStream[Any, Nothing, Event]
}

class MemoryEventJournal[K: Tag, Event](
                                    pollingInterval: Duration,
                                    internalStateEvents: Ref[Chunk[(Long, K, Long, Event, List[String])]],
                                    internalQueue: Queue[(K, Event)],
                                    clock: Clock
) extends EventJournal[K, Event]
    with JournalQuery[Long, K, Event]
    with TestEventStore[K, Event] {

  def getAppendedEvent(key: K): Task[List[Event]] = internalStateEvents.get.map { list =>
    list.collect {
      case (idSequence, innerKey, sequenceNumber, event, tags) if innerKey == key => event
    }.toList
  }

  def getAppendedStream(key: K): ZStream[Any, Nothing, Event] = ZStream.fromQueue(internalQueue).collect {
    case (internalKey, event) if internalKey == key => event
  }

  private val internal: ZRef[Any, Any, Nothing, Nothing, Chunk[(Long, K, Long, Event, List[String])], Map[K, Chunk[(Long, Long, Event, List[String])]]] = {
    internalStateEvents.map { elements =>
      elements
        .groupBy { element =>
          element._2
        }
        .view
        .mapValues { chunk =>
          chunk.map { case (idSequence, _, sequenceNumber, event, tags) =>
            (idSequence, sequenceNumber, event, tags)
          }
        }
        .toMap
    }
  }

  override def append(key: K, sequenceNr: Long, events: NonEmptyChunk[Event]) =
    ZIO.serviceWith[Tagging[K]] { tagging =>
      internalStateEvents.update { internalEvents =>
        val idSequenceToUse: Long = (internalEvents.lastOption.map(_._1).getOrElse(-1L)) + 1
        val tags = tagging.tag(key).map(_.value).toList
        internalEvents ++ events.zipWithIndex.map { case (event, index) =>
          (idSequenceToUse + index, key, index + sequenceNr, event, tags)
        }
      } *> internalQueue.offerAll(events.map(ev => key -> ev)).unit
    }

  override def read(key: K, sequenceNr: Long): stream.Stream[Throwable, EntityEvent[K, Event]] = {
    val a: UIO[List[EntityEvent[K, Event]]] = internal
      .map(_.getOrElse(key, Chunk.empty).toList.drop(sequenceNr.toInt).map { case (_, index, event, _) =>
        EntityEvent(key, index, event)
      })
      .get
    stream.Stream.fromIterableZIO(a)
  }

  override def eventsByTag(tag: EventTag, offset: Option[Long]): ZStream[Any, Throwable, JournalEntry[Long, K, Event]] = {
    (for {
      lastOffsetProcessed <- ZStream.fromZIO(Ref.make[Option[Long]](None))
      _                   <- ZStream.fromSchedule(Schedule.fixed(pollingInterval))
      lastOffset <- ZStream
        .fromZIO(lastOffsetProcessed.get)
      journalEntry <- currentEventsByTag(tag, lastOffset.orElse(offset)).mapZIO { event =>
        lastOffsetProcessed.set(Some(event.offset)).as(event)
      }
    } yield journalEntry).provideLayer(ZLayer.succeed(clock))
  }

  override def currentEventsByTag(tag: EventTag, offset: Option[Long]): stream.Stream[Throwable, JournalEntry[Long, K, Event]] = {
    val a: ZIO[Any, Nothing, List[JournalEntry[Long, K, Event]]] = internal.get.map { state =>
      state
        .flatMap { case (key, chunk) =>
          chunk.map { case (idSequence, sequenceNr, event, tags) =>
            (idSequence, key, sequenceNr, event, tags)
          }
        }
        .toList
        .sortBy(_._1)
        .drop(offset.map(_ + 1).getOrElse(0L).toInt)
        .collect {
          case (idSequence, key, sequenceNr, event, tagList) if tagList.contains(tag.value) =>
            JournalEntry(idSequence, EntityEvent(key, sequenceNr, event))
        }
    }
    stream.Stream.fromIterableZIO(a)
  }
}

object MemoryEventJournal {
  def make[Key: Tag, Event](pollingInterval: Duration): ZIO[Clock, Nothing, MemoryEventJournal[Key, Event]] = {
    for {
      internal <- Ref.make(Chunk[(Long, Key, Long, Event, List[String])]())
      queue    <- Queue.unbounded[(Key, Event)]
      clock    <- ZIO.service[Clock]
    } yield new MemoryEventJournal[Key, Event](pollingInterval, internal, queue, clock)
  }
}

object MemoryCommitableEventJournal {
  def memoryCommittableJournalStore[K: Tag, E: Tag]: ZIO[MemoryEventJournal[K, E], Nothing, CommittableJournalQuery[Long, K, E]] = {
    ZIO.service[MemoryEventJournal[K, E]].flatMap { eventJournalStore =>
      MemoryKeyValueStore.make[TagConsumer, Long].map { readSideOffsetStore =>
        new CommittableJournalStore[Long, K, E](readSideOffsetStore, eventJournalStore)
      }
    }
  }
}
