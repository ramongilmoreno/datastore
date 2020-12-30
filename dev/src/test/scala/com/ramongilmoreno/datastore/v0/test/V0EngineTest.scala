package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.Engine.{JDBCStatus, fieldValueName}
import com.ramongilmoreno.datastore.v0.implementation.QueryParser.Query
import com.ramongilmoreno.datastore.v0.implementation.{Engine, QueryParser}
import org.scalatest._

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

class V0EngineTest extends AsyncFlatSpec {

  class CustomJDBCStatus extends JDBCStatus {

    def q(query: Query): Future[Either[String, Exception]] = internalSQL(query)

    def u(record: Record): (RecordId, String, Seq[Any]) = internalUpdate(record)

    override def tableExists(table: TableId)(implicit ec: ExecutionContext): Future[Either[Boolean, Exception]] = Future(Left(true))(ec)

    override def columnsExists(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Set[(FieldId, Boolean)], Exception]] = Future {
      Left(columns.map((_, true)))
    }(ec)

    override def makeColumnsExist(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Unit, Exception]] = throw new UnsupportedOperationException

    override def connection: Connection = throw new UnsupportedOperationException

    override def makeTableExist(table: TableId)(implicit ec: ExecutionContext): Future[Either[Unit, Exception]] = throw new UnsupportedOperationException
  }

  "Engine" should "get simple SQL for a query without conditions" in {
    val query = "select a, b from c"
    val parsed = QueryParser.parse(query).get
    val result = new CustomJDBCStatus().q(parsed)
    val alias = Engine.tableAlias
    val aField = fieldValueName("a")
    val bField = fieldValueName("b")
    result.flatMap {
      case Left(sql) => Future(assert(sql == s"select $alias.$aField, $alias.$bField from c as $alias"))
      case Right(exception) => throw exception
    }
  }

  it should "be able of doing an insert" in {
    val u = new CustomJDBCStatus().u(Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), new RecordMetadata()))
    assert(u._2 == "insert into ? (?, ?, ?) values (?, ?, ?)")
    assert(u._3 == Seq("a", Engine.fieldValueName("b"), Engine.fieldValueName("c"), Engine.recordIdName(), "1", "2", u._1))
  }

  it should "be able of doing an update" in {
    val recordMeta = new RecordMetadata()
    val id = "x"
    recordMeta.id = Some(id)
    val u = new CustomJDBCStatus().u(Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), recordMeta))
    assert(u._2 == "update ? set ? = ?, ? = ? where ? = ?")
    assert(u._3 == Seq("a", Engine.fieldValueName("b"), "1", Engine.fieldValueName("c"), "2", Engine.recordIdName(), id))
  }
}
