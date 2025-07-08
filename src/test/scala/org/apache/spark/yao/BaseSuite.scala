package org.apache.spark.yao

import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

trait BaseSuite
  extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with Matchers