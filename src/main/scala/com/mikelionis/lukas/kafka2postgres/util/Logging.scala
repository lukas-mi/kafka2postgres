package com.mikelionis.lukas.kafka2postgres.util

import org.slf4j.LoggerFactory.getLogger
import org.slf4j.Logger

trait Logging {
  protected val log: Logger = getLogger(getClass)
}
