package com.retry.util

import java.io.{FileNotFoundException, IOException}
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Scheduler}
import com.retry.util.RetryUtil.RetryConfig
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RetryUtilSpec extends AnyWordSpecLike
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll {

  private val system = ActorSystem("test")

  override def afterAll(): Unit = system.terminate()

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(120, Seconds), interval = Span(20, Millis))

  // set a default predicate to retry for any exception in test
  private val defaultPredicate: Throwable => Boolean = _ => true

  "RetryUtil" should {
    implicit val scheduler: Scheduler = system.scheduler
    implicit val retryConfig: RetryConfig = RetryConfig()

    "run a successful Future immediately" in {
      val retried: Future[Int] =
        RetryUtil.retry[Int](Future.successful(5))
      retried.futureValue shouldBe 5
    }

    "run a successful Future only once" in {
      val counter = new AtomicInteger()
      val retried = RetryUtil.retry[Int] {
        Future.successful({
          counter.incrementAndGet()
        })
      }
      retried.futureValue shouldBe 1
    }

    "eventually return a failure for a Future that will never succeed" in {
      val retried =
        RetryUtil.retry(
          Future.failed(new IllegalStateException("Test Retry")),
          defaultPredicate
        )
      retried.failed.futureValue shouldBe a[IllegalStateException]
      retried.failed.futureValue.getMessage shouldBe "Test Retry"
    }

    "return a success for a Future that succeeds eventually" in {
      val failCount = new AtomicInteger()

      def attempt(): Future[Int] = {
        if (failCount.get() < 2) {
          failCount.incrementAndGet()
          Future.failed(new IllegalStateException(failCount.toString))
        } else Future.successful(5)
      }

      val retried = RetryUtil.retry(attempt(), defaultPredicate)

      retried.futureValue shouldBe 5

    }

    "return a failure for a Future that would have succeeded but retires were exhausted" in {
      val failCount = new AtomicInteger()

      def attempt(): Future[Int] = {
        if (failCount.get() < 4) {
          failCount.incrementAndGet()
          Future.failed(new IllegalStateException(failCount.toString))
        } else Future.successful(5)
      }

      val retried = RetryUtil.retry(attempt(), defaultPredicate)

      retried.failed.futureValue shouldBe a[IllegalStateException]
      retried.failed.futureValue.getMessage shouldBe 4.toString
    }

    "return a failure after retires were exhausted with configured numRetries" in {
      val failCount = new AtomicInteger()

      def attempt(): Future[Int] = {
        failCount.incrementAndGet()
        Future.failed(new IllegalStateException(failCount.toString))
      }

      val result = RetryUtil.retry[Int](attempt, defaultPredicate)
      result.failed.futureValue shouldBe a[IllegalStateException]
      //validate its retries for 3 time + the actual call
      result.failed.futureValue.getMessage shouldBe "4"
    }

    "return a failure after retires for runtime exception when predicate is to configured to test runtime exception" in {
      val failCount = new AtomicInteger()

      def attempt(): Future[Int] = {
        failCount.incrementAndGet()
        Future.failed(new IllegalStateException(failCount.toString))
      }

      val result = RetryUtil.retry[Int](
        attempt,
        (e: Throwable) => e.isInstanceOf[RuntimeException]
      )
      result.failed.futureValue shouldBe a[RuntimeException]
      //validate its retries for 3 time + the actual call
      result.failed.futureValue.getMessage shouldBe "4"
    }

    "return a failure immediately when predicate is not configured to test runtime exception" in {
      val failCount = new AtomicInteger()

      def attempt(): Future[Int] = {
        failCount.incrementAndGet()
        Future.failed(new IllegalStateException(failCount.toString))
      }

      val result = RetryUtil.retry[Int](
        attempt,
        (e: Throwable) => e.isInstanceOf[IllegalArgumentException]
      )
      result.failed.futureValue shouldBe a[RuntimeException]

      //validate its has only actual call no retries
      result.failed.futureValue.getMessage shouldBe "1"
    }

    "return a failure after retires when global predicate used with configured retryable exception and one of its sub class is thrown" in {
      val failCount = new AtomicInteger()

      def attempt(): Future[Int] = {
        failCount.incrementAndGet()
        Future.failed(new FileNotFoundException(failCount.toString))
      }

      val result = RetryUtil.retry[Int](attempt)
      result.failed.futureValue shouldBe a[FileNotFoundException]

      result.failed.futureValue.getMessage shouldBe "4"
    }

  }

}
