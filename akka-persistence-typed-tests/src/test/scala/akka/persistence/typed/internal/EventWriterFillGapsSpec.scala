/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.pattern.StatusReply
import akka.persistence.AtomicWrite
import akka.persistence.JournalProtocol

object EventWriterFillGapsSpec {
  def config =
    ConfigFactory.parseString("""
      akka.persistence.journal.inmem.delay-writes=10ms
    """).withFallback(ConfigFactory.load()).resolve()
}

class EventWriterFillGapsSpec
    extends ScalaTestWithActorTestKit(EventWriterFillGapsSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  val settings = EventWriter.EventWriterSettings(
    10,
    5.seconds,
    fillSequenceNumberGaps = true,
    latestSequenceNumberCacheCapacity = 100)
  implicit val ec: ExecutionContext = testKit.system.executionContext

  "The event writer" should {

    "handle events without gaps when starting at 1" in new TestSetup {
      sendWrite(1)
      journalAckWrite()
      clientExpectSuccess(1)

      sendWrite(2)
      journalAckWrite()
      clientExpectSuccess(1)

      sendWrite(3)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "handle events without gaps when starting at > 1" in new TestSetup {
      sendWrite(3)
      journalHighestSeqNr(2)
      journalAckWrite()
      clientExpectSuccess(1)

      sendWrite(4)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "handle duplicates" in new TestSetup {
      sendWrite(1)
      journalAckWrite()
      clientExpectSuccess(1)

      // should also be ack:ed
      sendWrite(1)
      // duplicates detected before journal write
      fakeJournal.expectNoMessage()
      clientExpectSuccess(1)
    }

    "handle batched duplicates" in new TestSetup {
      // first write, and wait for ack so that this test doesn't have to lookup latest seqNr
      sendWrite(1)
      journalAckWrite()
      clientExpectSuccess(1)

      sendWrite(2)

      // first batch
      for (n <- 3L to 10L) {
        sendWrite(n)
      }
      // 2 will be written directly
      journalAckWrite() should ===(1)
      clientExpectSuccess(1)

      // completing 1 triggers write of batch with 3-10
      // second
      for (n <- 1L to 10L) {
        sendWrite(n)
      }
      // batch 3-10 in flight, writes in the meanwhile go in a new batch
      journalAckWrite() should ===(8)
      // duplicates detected before journal write

      clientExpectSuccess(18)
    }

    "handle batches with half duplicates" in new TestSetup {
      // first write, and wait for ack so that this test doesn't have to lookup latest seqNr
      sendWrite(1)
      journalAckWrite()
      clientExpectSuccess(1)

      sendWrite(1)
      for (n <- 2L to 10L) {
        sendWrite(n)
      }
      journalAckWrite() should ===(1)
      journalAckWrite() should ===(8)
      clientExpectSuccess(9)

      for (n <- 5L until 15L) {
        sendWrite(n)
      }
      // duplicates detected before journal write
      journalAckWrite() should ===(1) // new write of 11 (non duplicates)
      journalAckWrite() should ===(3) // new write of 12-15 (non duplicates)

      // all writes succeeded
      clientExpectSuccess(10)
    }

    "pass real errors from journal back" in new TestSetup {
      sendWrite(1L)
      journalFailWrite("error error")
      // duplicate handling will ask for highest seq nr, can't know it is an actual error
      journalHighestSeqNr(0L)
      val response = clientProbe.receiveMessage()
      response.isError should ===(true)
      response.getError.getMessage should ===("Journal write failed")
    }

    "pass real errors from journal back when highestSeqNr fails" in new TestSetup {
      sendWrite(1L)
      journalFailWrite("the error")
      // duplicate handling will ask for highest seq nr, can't know it is an actual error
      journalFailHighestSeqNr("highest error")
      val response = clientProbe.receiveMessage()
      response.isError should ===(true)
      response.getError.getMessage should ===("Journal write failed")
    }

    "fill gaps when next expected is known" in new TestSetup {
      sendWrite(1)
      sendWrite(5)
      journalAckWrite()
      clientExpectSuccess(1)
      journalAckWrite(expectedSequenceNumbers = Vector(2, 3, 4, 5))
      clientExpectSuccess(1)

      sendWrite(6)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "fill gaps when next expected is unknown" in new TestSetup {
      sendWrite(5)
      journalHighestSeqNr(2)
      journalAckWrite(expectedSequenceNumbers = Vector(3, 4, 5))
      clientExpectSuccess(1)

      sendWrite(6)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "fill gaps when next expected is not trusted" in new TestSetup {
      sendWrite(1)
      journalAckWrite()
      clientExpectSuccess(1)

      // event though we know about 1 some other node might have written 2, 3, ...
      sendWrite(5)
      journalHighestSeqNr(2)
      journalAckWrite(expectedSequenceNumbers = Vector(3, 4, 5))
      clientExpectSuccess(1)

      sendWrite(6)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "fill gaps when next expected is unknown and no highest" in new TestSetup {
      sendWrite(5)
      journalHighestSeqNr(0) // no events stored
      journalAckWrite(expectedSequenceNumbers = Vector(1, 2, 3, 4, 5))
      clientExpectSuccess(1)

      sendWrite(6)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "fill gaps when next expected is unknown and more writes afterwards" in new TestSetup {
      sendWrite(4)
      sendWrite(5)
      journalHighestSeqNr(2)
      journalAckWrite(expectedSequenceNumbers = Vector(3, 4))
      journalAckWrite(expectedSequenceNumbers = Vector(5))
      clientExpectSuccess(2)

      sendWrite(6)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "fill gaps when pending write" in new TestSetup {
      sendWrite(1)

      // no ack of 1 yet, but since it's pending we don't have to lookup max
      sendWrite(5)
      journalAckWrite() shouldBe 1
      journalAckWrite() shouldBe 4
      clientExpectSuccess(2)

      sendWrite(6)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "fill gaps when pending batch" in new TestSetup {
      sendWrite(1)
      // 2 and 3 in next batch
      sendWrite(2)
      sendWrite(3)

      sendWrite(5)
      journalAckWrite() shouldBe 1
      journalAckWrite() shouldBe 4 // all go into next batch, including the filled 4
      clientExpectSuccess(4)

      sendWrite(6)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "fill gaps when more writes after max lookup" in new TestSetup {
      sendWrite(5)
      sendWrite(6)
      sendWrite(7)
      sendWrite(9) // 8 is another gap
      journalHighestSeqNr(3) // 4 is a gap
      journalAckWrite(expectedSequenceNumbers = Vector(4L, 5L))
      // remaining into next batch
      journalAckWrite() shouldBe 4
      clientExpectSuccess(4)

      sendWrite(10)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "fill gaps when duplicate detected by journal" in new TestSetup {
      sendWrite(1)
      // 2 and 3 in next batch
      sendWrite(2)
      sendWrite(3)

      sendWrite(5)
      journalAckWrite(expectedSequenceNumbers = Vector(1))
      // let's say 2 is a duplicate, detected by journal
      journalFailWrite("duplicate") shouldBe 4
      journalHighestSeqNr(2)
      journalAckWrite(expectedSequenceNumbers = Vector(3, 4, 5))
      clientExpectSuccess(4)

      sendWrite(6)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "not count filled gaps in max batch check" in new TestSetup {
      settings.maxBatchSize should ===(10)
      sendWrite(1)

      sendWrite(15)
      sendWrite(16)
      journalAckWrite(expectedSequenceNumbers = Vector(1))
      journalAckWrite(expectedSequenceNumbers = (2L to 16L).toVector)
      clientExpectSuccess(2)

      sendWrite(17)
      journalAckWrite()
      clientExpectSuccess(1)
    }

    "evict least recently used entries" in new TestSetup {
      // this test is based on capacity of 100
      settings.latestSequenceNumberCacheCapacity should ===(100)
      (1 to 13).foreach { n =>
        sendWrite(1, pid = s"pid$n")
        journalAckWrite(s"pid$n")
      }
      Thread.sleep(100) // makes the first 13 least recently used

      (14 to 110).foreach { n =>
        sendWrite(1, pid = s"pid$n")
        journalAckWrite(s"pid$n")
      }

      // touch pid1
      sendWrite(2, "pid1")
      journalAckWrite("pid1")
      // touch pid2
      sendWrite(2, "pid2")
      journalAckWrite("pid2")
      // one more
      sendWrite(1, "pid111")
      journalAckWrite(pid = "pid111")

      // now exceeded the cache threshold, and evicted entries will result in lookup
      sendWrite(2, pid = "pid3")
      journalHighestSeqNr(1)
      journalAckWrite("pid3")
      sendWrite(2, pid = "pid12")
      journalHighestSeqNr(1)
      journalAckWrite("pid12")
      sendWrite(2, pid = "pid13")
      journalHighestSeqNr(1)
      journalAckWrite("pid13")

      // still in cache
      sendWrite(2, "pid14")
      journalAckWrite("pid14")
      sendWrite(3, "pid1")
      journalAckWrite("pid1")
      sendWrite(3, "pid2")
      journalAckWrite("pid2")
    }

    "handle writes to many pids" in {
      // no flow control in this test so just no limit on batch size
      val writer = spawn(EventWriter("akka.persistence.journal.inmem", settings.copy(maxBatchSize = Int.MaxValue)))
      val probe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
      (1 to 1000).map { pidN =>
        Future {
          for (n <- 1 to 20) {
            writer ! EventWriter.Write(s"A|pid$pidN", n.toLong, n.toString, false, None, Set.empty, probe.ref)
          }
        }
      }
      val replies = probe.receiveMessages(20 * 1000, 20.seconds)
      replies.exists(_.isError) should ===(false)
    }

    "handle writes to many pids and fill gaps" in {
      // no flow control in this test so just no limit on batch size
      val writer = spawn(EventWriter("akka.persistence.journal.inmem", settings.copy(maxBatchSize = Int.MaxValue)))
      val probe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
      (1 to 1000).map { pidN =>
        Future {
          for (n <- 1 to 20) {
            val gap =
              if (pidN <= 500 && n <= 3) true
              else if (pidN > 500 && 9 <= n && n <= 11) true
              else false

            if (!gap)
              writer ! EventWriter.Write(s"B|pid$pidN", n.toLong, n.toString, false, None, Set.empty, probe.ref)
          }
        }
      }
      // 20 - 3 because 3 gaps for each pid
      val replies = probe.receiveMessages((20 - 3) * 1000, 20.seconds)
      replies.exists(_.isError) should ===(false)
    }
  }

  trait TestSetup {
    def pid1 = "pid1"
    val fakeJournal = createTestProbe[JournalProtocol.Message]()
    val writer = spawn(EventWriter(fakeJournal.ref, settings))
    val clientProbe = createTestProbe[StatusReply[EventWriter.WriteAck]]()
    def sendWrite(seqNr: Long, pid: String = pid1): Unit = {
      writer ! EventWriter.Write(pid, seqNr, seqNr.toString, false, None, Set.empty, clientProbe.ref)
    }
    def journalAckWrite(pid: String = pid1, expectedSequenceNumbers: Vector[Long] = Vector.empty): Int = {
      val write = fakeJournal.expectMessageType[JournalProtocol.WriteMessages]
      write.messages should have size (1)
      val atomicWrite = write.messages.head.asInstanceOf[AtomicWrite]

      val seqNrs =
        atomicWrite.payload.map { repr =>
          repr.persistenceId should ===(pid)
          write.persistentActor ! JournalProtocol.WriteMessageSuccess(repr, write.actorInstanceId)
          repr.sequenceNr
        }
      if (expectedSequenceNumbers.nonEmpty)
        seqNrs should ===(expectedSequenceNumbers)
      write.persistentActor ! JournalProtocol.WriteMessagesSuccessful
      atomicWrite.payload.size
    }

    def journalFailWrite(reason: String, pid: String = pid1): Int = {
      val write = fakeJournal.expectMessageType[JournalProtocol.WriteMessages]
      write.messages should have size (1)
      val atomicWrite = write.messages.head.asInstanceOf[AtomicWrite]
      atomicWrite.payload.foreach { repr =>
        repr.persistenceId should ===(pid)
        write.persistentActor ! JournalProtocol.WriteMessageFailure(repr, TestException(reason), write.actorInstanceId)
      }
      write.persistentActor ! JournalProtocol.WriteMessagesFailed
      atomicWrite.payload.size
    }

    def journalHighestSeqNr(highestSeqNr: Long): Unit = {
      val replay = fakeJournal.expectMessageType[JournalProtocol.ReplayMessages]
      replay.persistentActor ! JournalProtocol.RecoverySuccess(highestSeqNr)
    }

    def journalFailHighestSeqNr(reason: String): Unit = {
      val replay = fakeJournal.expectMessageType[JournalProtocol.ReplayMessages]
      replay.persistentActor ! JournalProtocol.ReplayMessagesFailure(TestException(reason))
    }

    def clientExpectSuccess(n: Int) = {
      clientProbe.receiveMessages(n).foreach { reply =>
        reply.isSuccess should be(true)
      }
    }
  }

}
