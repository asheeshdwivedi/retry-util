package com.retry.util.log

import org.slf4j.Logger
import org.slf4j.LoggerFactory

trait Logging {
  val log: Logger = LoggerFactory.getLogger(getClass)
}
