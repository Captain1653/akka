/*
 * Copyright (C) 2015-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit.scaladsl

import akka.actor.ClassicActorSystemProvider
import akka.stream._
import akka.stream.Attributes.none
import akka.stream.scaladsl._
import akka.stream.testkit._
import akka.stream.testkit.StreamTestKit.ProbeSource

/**
 * Factory methods for test sources.
 */
object TestSource {

  /**
   * A Source that materializes to a [[akka.stream.testkit.TestPublisher.Probe]].
   */
  def probe[T](implicit system: ClassicActorSystemProvider) =
    Source.fromGraph[T, TestPublisher.Probe[T]](new ProbeSource(none, SourceShape(Outlet("ProbeSource.out"))))

}
