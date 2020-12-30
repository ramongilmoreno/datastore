package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.Engine.H2Status
import com.ramongilmoreno.datastore.v0.implementation.QueryParser
import org.scalatest._

import java.sql.{Connection, DriverManager}
import java.util.UUID
import scala.concurrent.Future

class V0EngineTest extends AsyncFlatSpec {

  class CustomJDBCStatus extends H2Status {

    Class.forName("org.h2.Driver")
    private val url = "jdbc:h2:mem:" + UUID.randomUUID().toString
    private val conn = DriverManager.getConnection(url)

    override def connection: Connection = conn
  }

  "Engine" should "get simple SQL for a query without conditions" in {
    val query = "select a, b from c"
    val parsed = QueryParser.parse(query).get
    val status = new CustomJDBCStatus()
    status.query(parsed)
      .flatMap {
        case Left(result) =>
          assert(result.columns == parsed.fields)
          assert(result.rows.length == 0)
        case Right(exception) =>
          fail(exception)
      }
  }

  it should "be able of doing an insert" in {
    val record = Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), new RecordMetadata())
    val status = new CustomJDBCStatus()
    status.update(List(record))
      .flatMap {
        case Left(result) =>
          val id = result.head
          status.query(QueryParser.parse("select b, c from a").get)
            .flatMap {
              case Left(result) =>
                Future {
                  assert(result.columns == List[FieldId]("b", "c"))
                  assert(result.rows.length == 1)
                }
              case Right(exception) =>
                fail(exception)
            }
        case Right(exception) =>
          fail(exception)
      }
  }
}
