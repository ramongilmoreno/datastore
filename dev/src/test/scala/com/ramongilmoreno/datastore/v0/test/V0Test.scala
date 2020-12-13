package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.implementation.QueryParser
import com.ramongilmoreno.datastore.v0.implementation.QueryParser._
import org.scalatest._

class V0Test extends FlatSpec {

  "Parser" should "parse a query without conditions" in {
    val query = "select a, b from c"
    val result = Query(
      List(
        Field("a"),
        Field("b")
      ),
      "c",
      None
    )
    assert(QueryParser.parse(query).get == result)
  }

  it should "parse a query with several conditions" in {
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

  it should "parse a query just with OR" in {
    val query =
      """
        |select
        |  a
        |from
        |  b
        |where
        |  c = d or
        |  e = f
        |""".stripMargin
    val result = Query(
      List(
        Field("a"),
      ),
      "b",
      Some(
        OrCondition(
          SingleCondition(Field("c"), Equal, Field("d")),
          SingleCondition(Field("e"), Equal, Field("f"))
        )
      )
    )
    assert(QueryParser.parse(query).get == result)
  }
}
