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

    override def columnExists(table: TableId, column: FieldId)(implicit ec: ExecutionContext): Future[Boolean] = Future(true)(ec)

    override def tableExists(table: TableId)(implicit ec: ExecutionContext): Future[Boolean] = Future(true)(ec)

    override def connection(): Future[Connection] = throw new UnsupportedOperationException

    def q(query: Query): Future[String] = internalSQL(query)

    def u(table: TableId, record: Record): (String, Seq[Any]) = internalUpdate(table, record)
  }

  "Engine" should "get simple SQL for a query without conditions" in {
    val query = "select a, b from c"
    val parsed = QueryParser.parse(query).get
    val result: Future[String] = new CustomJDBCStatus().q(parsed)
    val alias = Engine.tableAlias
    val aField = fieldValueName("a")
    val bField = fieldValueName("b")
    result.flatMap(sql => assert(sql == s"select $alias.$aField, $alias.$bField from c as $alias"))
  }

  it should "be able of doing an insert" in {
    val u = new CustomJDBCStatus().u("a", Record(Map("b" -> FieldData("1"), "c" -> FieldData("2")), new RecordMetadata()))
    assert(u._1 == "insert into ? (?, ?, ?) values (?, ?, ?)")
    val id = u._2.last
    assert(u._2 == Seq("a", Engine.fieldValueName("b"), Engine.fieldValueName("c"), Engine.recordIdName(), "1", "2", id))
  }

  it should "be able of doing an update" in {
    val recordMeta = new RecordMetadata()
    val id = "x"
    recordMeta.id = Some(id)
    val u = new CustomJDBCStatus().u("a", Record(Map("b" -> FieldData("1"), "c" -> FieldData("2")), recordMeta))
    assert(u._1 == "update ? set ? = ?, ? = ? where ? = ?")
    assert(u._2 == Seq("a", Engine.fieldValueName("b"), "1", Engine.fieldValueName("c"), "2", Engine.recordIdName(), id))
  }
}
