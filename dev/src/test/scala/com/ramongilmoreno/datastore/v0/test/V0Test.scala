package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.implementation.QueryParser
import com.ramongilmoreno.datastore.v0.implementation.QueryParser._
import org.scalatest._

class V0Test extends FlatSpec {
  "Parser" should "parse a simple query" in {
    val query =
      """
        |select
        |  a,
        |  b
        |from
        |  c
        |where
        |  d = e and
        |  (
        |    "f" <> g or
        |    h = "i"
        |  )
        |
        |""".stripMargin
    val result = Query(
      List(
        Field("a"),
        Field("b")
      ),
      "c",
      Some(
        AndCondition(
          SingleCondition(Field("d"), Equal, Field("e")),
          OrCondition(
            SingleCondition(Value("f"), NotEqual, Field("g")),
            SingleCondition(Field("h"), Equal, Value("i")),
          )
        )
      )
    )
    assert(QueryParser.parse(query).get == result)
  }
}
