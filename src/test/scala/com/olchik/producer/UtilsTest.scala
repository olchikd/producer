package com.olchik.producer

import org.scalatest.FunSuite


class RandomUtilTest extends FunSuite {
  test("notMoreThan"){
    assertThrows[IllegalArgumentException] {
      RandomUtils.notMoreThan(0)
    }

    assert(RandomUtils.notMoreThan(1) == 1)
    val value = RandomUtils.notMoreThan(2)
    assert(value <= 2)
    assert(value >= 1)
  }

  test("fromSeq"){
    assertThrows[IllegalArgumentException] {
      RandomUtils.fromSeq(Nil)
    }

    assert(RandomUtils.fromSeq(100 :: Nil) == 100)
  }
}