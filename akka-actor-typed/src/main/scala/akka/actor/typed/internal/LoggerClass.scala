/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import scala.util.control.NonFatal
import akka.annotation.InternalApi

import java.lang.StackWalker.StackFrame
import scala.compat.java8.OptionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object LoggerClass {

  private val defaultPrefixesToSkip = List("scala.runtime", "akka.actor.typed.internal")

  /**
   * Try to extract a logger class from the call stack, if not possible the provided default is used
   */
  def detectLoggerClassFromStack(default: Class[_], additionalPrefixesToSkip: List[String] = Nil): Class[_] = {
    try {
      val allPrefixesToSkip = additionalPrefixesToSkip ::: defaultPrefixesToSkip

      def shouldSkipStackFrame(stackFrame: StackFrame) = {
        allPrefixesToSkip.exists(item => item.startsWith(stackFrame.getDeclaringClass.getName))
      }

      StackWalker
        .getInstance()
        .walk(
          frames =>
            frames
              .skip(1)// skip this method/class and right away
              .dropWhile(stackFrame => shouldSkipStackFrame(stackFrame))
              .findFirst()
              .map(stackFrame => stackFrame.getDeclaringClass))
        .asScala
        .getOrElse(default)
    } catch {
      case NonFatal(_) => default
    }
  }

}
