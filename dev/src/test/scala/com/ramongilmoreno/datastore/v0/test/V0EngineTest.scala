package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.Engine.{InMemoryH2Status, TransactionBad, TransactionCondition, TransactionGood, flatMapRightWrapper}
import com.ramongilmoreno.datastore.v0.implementation.QueryParser
import org.scalatest._

import scala.concurrent.Future

class V0EngineTest extends AsyncFlatSpec {

  def sampleRecordExpires(delta: Long): Record =
    sampleRecord(Some(System.currentTimeMillis() + delta))

  def sampleRecord(expires: Option[Timestamp] = None): Record = {
    val meta = RecordMetadata(None, expires)
    Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), meta)
  }

  def sampleRecordData(value: String, value2: String = "x"): Record = {
    val meta = RecordMetadata()
    Record("a", Map("b" -> FieldData(value), "c" -> FieldData(value2)), meta)
  }

  "Engine" should "get simple SQL for a query without conditions" in {
    val query = "select a, b from c"
    val parsed = QueryParser.parse(query).get
    val status = new InMemoryH2Status()
    status.query(parsed)
      .flatMap {
        case Right(result) =>
          assert(result.columns == parsed.fields)
          assert(result.rows.length == 0)
        case Left(exception) =>
          fail(exception)
      }
  }

  it should "be able of doing an insert" in {
    val status = new InMemoryH2Status()
    status.update(List(sampleRecord()))
      .flatMap {
        case Right(_) =>
          status.query(QueryParser.parse("select b, c from a").get)
            .flatMap {
              case Right(result) =>
                Future {
                  assert(result.columns == List[FieldId]("b", "c"))
                  assert(result.rows.length == 1)
                  assert(result.meta(0).expires.isEmpty)
                }
              case Left(exception) =>
                fail(exception)
            }
        case Left(exception) =>
          fail(exception)
      }
  }


  it should "be able of ignoring stale records" in {
    val status = new InMemoryH2Status()
    val delta = 60 * 1000
    val stale = sampleRecordExpires(-delta)
    val current = sampleRecordExpires(delta)
    status.update(List(stale, current))
      .flatMap {
        case Right(_) =>
          status.query(QueryParser.parse("select b, c from a").get)
            .flatMap {
              case Right(result) =>
                Future {
                  assert(result.rows.length == 1)
                  assert(result.meta(0).expires == current.meta.expires)
                }
              case Left(exception) =>
                fail(exception)
            }
        case Left(exception) =>
          fail(exception)
      }
  }

  it should "be able of ignoring deleted records" in {
    val status = new InMemoryH2Status()
    val record = sampleRecord()
    status.update(List(record)).flatMapRight(ids => {
      val id = ids.head
      val q = QueryParser.parse("select b, c from a").get
      status.query(q)
        .flatMapRight(found => {
          assert(found.rows.length == 1)
          val delete = RecordMetadata(Some(id), None, active = false)
          status.update(List(Record("a", Map.empty, delete)))
            .flatMapRight(_ => {
              status.query(q)
                .flatMapRight(found2 => {
                  Future {
                    Right(assert(found2.rows.length == 0))
                  }
                })
            })
        })
    })
      .flatMap {
        case Left(e) => fail(e)
        case Right(x) => Future(x)
      }
  }


  it should "be able of querying with conditions" in {
    val status = new InMemoryH2Status()
    status.update((1 to 3).map(i => sampleRecordData(i.toString)).toList)
      .flatMap {
        case Right(_) =>
          status.query(QueryParser.parse("""select b from a where b = "1" or (b = "3")""").get)
            .flatMap {
              case Right(result) =>
                Future {
                  assert(result.rows.length == 2)
                  assert(Set(result.value(0, "b"), result.value(1, "b")) == Set("1", "3"))
                }
              case Left(exception) =>
                fail(exception)
            }
        case Left(exception) =>
          fail(exception)
      }
  }

  it should "be able of querying with complex conditions" in {
    val status = new InMemoryH2Status()
    val records = for (
      b <- 1 to 3;
      c <- List("a", "b", "c")
    ) yield {
      sampleRecordData(b.toString, c)
    }
    status.update(records.toList)
      .flatMap {
        case Right(_) =>
          status.query(QueryParser.parse("""select b, c from a where b = "2" and (c = "a" or c = "c")""").get)
            .flatMap {
              case Right(result) =>
                Future {
                  assert(result.rows.length == 2)
                  assert(result.value(0, "b") == "2")
                  assert(result.value(1, "b") == "2")
                  assert(Set(result.value(0, "c"), result.value(1, "c")) == Set("a", "c"))
                }
              case Left(exception) =>
                fail(exception)
            }
        case Left(exception) =>
          fail(exception)
      }
  }

  it should "check transactions OK" in {
    val status = new InMemoryH2Status()
    val records = for (
      b <- 1 to 3;
      c <- List("a", "b", "c")
    ) yield {
      sampleRecordData(b.toString, c)
    }
    status.update(records.toList)
      .flatMapRight(_ => {
        val expected = Seq(Record("a", Map("b" -> FieldData("2"))))
        val q = QueryParser.parse("""select b from a where b = "2" and c = "a"""").get
        status.checkTransactionConditions(Seq(new TransactionCondition(q, expected)))
      })
      .flatMapRight {
        case TransactionGood() => Future(Right(succeed))
        case TransactionBad(_) => Future(Right(fail()))
      }
      .flatMap {
        case Left(exception) => fail(exception)
        case Right(assertion) => assertion
      }
  }

  it should "check transactions KO" in {
    val status = new InMemoryH2Status()
    val records = for (
      b <- 1 to 3;
      c <- List("a", "b", "c")
    ) yield {
      sampleRecordData(b.toString, c)
    }
    status.update(records.toList)
      .flatMapRight(_ => {
        val expected = Seq(Record("a", Map("b" -> FieldData("3"))))
        val q = QueryParser.parse("""select b from a where b = "2" and c = "a"""").get
        status.checkTransactionConditions(Seq(new TransactionCondition(q, expected)))
      })
      .flatMapRight {
        case TransactionGood() => Future(Right(fail()))
        case TransactionBad(_) => Future(Right(succeed))
      }
      .flatMap {
        case Left(exception) => fail(exception)
        case Right(assertion) => assertion
      }
  }
}