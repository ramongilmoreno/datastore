package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.Engine.H2Status
import com.ramongilmoreno.datastore.v0.implementation.QueryParser
import org.scalatest._

import java.sql.{Connection, DriverManager}
import java.util.UUID
import scala.concurrent.Future

class V0EngineTest extends AsyncFlatSpec {

  def sampleRecordExpires(delta: Long): Record =
    sampleRecord(Some(System.currentTimeMillis() + delta))

  def sampleRecord(expires: Option[Timestamp] = None): Record = {
    val meta = new RecordMetadata()
    meta.expires = expires
    Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), meta)
  }

  def sampleRecordData(value: String): Record = {
    val meta = new RecordMetadata()
    Record("a", Map("b" -> FieldData(value)), meta)
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

  class CustomJDBCStatus extends H2Status {
    Class.forName("org.h2.Driver")
    private val url = "jdbc:h2:mem:" + UUID.randomUUID().toString
    private val conn = DriverManager.getConnection(url)

    override def connection: Connection = conn
  }

  it should "be able of doing an insert" in {
    val status = new CustomJDBCStatus()
    status.update(List(sampleRecord()))
      .flatMap {
        case Left(_) =>
          status.query(QueryParser.parse("select b, c from a").get)
            .flatMap {
              case Left(result) =>
                Future {
                  assert(result.columns == List[FieldId]("b", "c"))
                  assert(result.rows.length == 1)
                  assert(result.meta(0).expires.isEmpty)
                }
              case Right(exception) =>
                fail(exception)
            }
        case Right(exception) =>
          fail(exception)
      }
  }


  it should "be able of ignoring stale records" in {
    val status = new CustomJDBCStatus()
    val delta = 60 * 1000
    val stale = sampleRecordExpires(-delta)
    val current = sampleRecordExpires(delta)
    status.update(List(stale, current))
      .flatMap {
        case Left(_) =>
          status.query(QueryParser.parse("select b, c from a").get)
            .flatMap {
              case Left(result) =>
                Future {
                  assert(result.rows.length == 1)
                  assert(result.meta(0).expires == current.meta.expires)
                }
              case Right(exception) =>
                fail(exception)
            }
        case Right(exception) =>
          fail(exception)
      }
  }

  it should "be able of querying with conditions" in {
    val status = new CustomJDBCStatus()
    status.update((1 to 3).map(i => sampleRecordData(i.toString)).toList)
      .flatMap {
        case Left(_) =>
          status.query(QueryParser.parse("""select b from a where b = "1" or (b = "3")""").get)
            .flatMap {
              case Left(result) =>
                Future {
                  assert(result.rows.length == 2)
                  assert(Set(result.value(0, "b"), result.value(1, "b")) == Set("1", "3"))
                }
              case Right(exception) =>
                fail(exception)
            }
        case Right(exception) =>
          fail(exception)
      }
  }
}