package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.implementation.QueryParser
import com.ramongilmoreno.datastore.v0.implementation.QueryParser._
import org.scalatest._

class V0Test extends FlatSpec {
  "Parser" should "parse a simple query" in {
    val query = """select a, b from c where d = e and f <> "g" and "h" = i"""
    val result = Query(
      List(
        Field("a"),
        Field("b")
      ),
      "c",
      List(
        Condition(Field("d"), Equal, Field("e")),
        Condition(Field("f"), NotEqual, Value("g")),
        Condition(Value("h"), Equal, Field("i"))
      )
    )
    assert(QueryParser.parse(query).get == result)
  }
}
