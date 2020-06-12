package com.retry.util

import java.io.IOException
import java.util.concurrent.ThreadLocalRandom

import akka.actor.{ActorLogging, Scheduler}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import akka.pattern._
import com.retry.util.log.Logging

object RetryUtil extends Logging {

  val RetryableExceptions: Array[_ >: Throwable] = Array(
    classOf[IllegalArgumentException],
    classOf[IOException]
  )

  val defaultPredicate: Throwable => Boolean = e =>
    RetryableExceptions.map(_.equals(e.getClass))
      .reduce(_ || _)

  case class RetryConfig(
                          numRetries: Long = 3,
                          firstBackoff: FiniteDuration = 1.second,
                          maxBackoff: FiniteDuration = 10.second,
                          jitterFactor: Double = 0.5
                        ) {
    if (numRetries <= 0)
      throw new IllegalArgumentException("numRetries must be greater then 0")
    if (firstBackoff == null)
      throw new IllegalArgumentException("firstBackoff can not be null")
    if (maxBackoff == null)
      throw new IllegalArgumentException("maxBackoff can not be null")
    if (jitterFactor < 0 || jitterFactor > 1)
      throw new IllegalArgumentException(
        "jitterFactor must be between 0 and 1"
      )
  }

  /**
   * retry function with exponential backoff delay. Retries are performed after a backoff
   * * interval of <code>firstBackoff * (factor ** n) + jitter</code> where n is the iteration.
   */
  def retry[T](
                attempt: => Future[T],
                predicate: Throwable => Boolean = defaultPredicate
              )(implicit
                ec: ExecutionContext,
                scheduler: Scheduler,
                retryConfig: RetryConfig
              ): Future[T] = {

    def retry(
               attempt: => Future[T],
               delay: FiniteDuration,
               iteration: Long = 1
             ): Future[T] = {
      try {
        if (retryConfig.numRetries >= iteration) {
          attempt.recoverWith {
            case NonFatal(e) if predicate(e) =>
              val nextBackoff = nextBackOff(delay, iteration, retryConfig)
              log.debug(
                "Exception caught {}, message {} scheduling attempt to run in {} seconds",
                e.getClass.getSimpleName,
                e.getMessage,
                nextBackoff.toSeconds
              )
              after(nextBackoff, scheduler) {
                log.debug(
                  "Exception caught {}, message {}, retrying exponentially, iteration: {}",
                  e.getClass.getSimpleName,
                  e.getMessage,
                  iteration
                )
                retry(attempt, nextBackoff, iteration + 1)
              }
            case NonFatal(e) =>
              log.error(
                "Non Retryable  Exception caught {}, message {} ",
                e.getClass.getSimpleName,
                e.getMessage
              )
              Future.failed(e)
          }
        } else {
          log.info(
            s"Max Retry: ${retryConfig.numRetries} exhausted returning actual attempt"
          )
          attempt
        }
      } catch {
        case NonFatal(error) =>
          log.error(
            s"Unhandled exception caught exhausted",
            error
          )
          Future.failed(error)
      }
    }

    retry(attempt, retryConfig.firstBackoff)
  }

  def nextBackOff(
                   delay: FiniteDuration,
                   iteration: Long,
                   retryConfig: RetryConfig
                 ): FiniteDuration = {
    val backoff: FiniteDuration = delay * Math
      .pow(2, iteration - 1)
      .toLong
    println("Back off " + backoff)
    addJitter(retryConfig, backoff)
  }

  /**
   * offsetCap is used for the bounds of the jitter
   * lowBound for the jitter that won't let the final backoff go below [[RetryConfig.firstBackoff]].
   * highBound for the jitter that won't let the final backoff go over [[RetryConfig.maxBackoff]].
   */
  def addJitter(
                 retryConfig: RetryConfig,
                 duration: FiniteDuration
               ): FiniteDuration = {
    val random = ThreadLocalRandom.current

    val offsetCap =
      (duration * (100 * retryConfig.jitterFactor) / 100).toMillis

    val lowBound =
      Math.max((retryConfig.firstBackoff - duration).toMillis, -offsetCap)

    val highBound =
      Math.min((retryConfig.maxBackoff - duration).toMillis, offsetCap)

    val jitter =
      if (highBound <= lowBound)
        if (highBound <= 0) 0
        else random.nextLong(highBound)
      else random.nextLong(lowBound, highBound)

    duration + jitter.millis
  }
}
