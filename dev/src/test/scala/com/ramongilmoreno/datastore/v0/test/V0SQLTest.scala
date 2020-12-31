package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.Engine._
import com.ramongilmoreno.datastore.v0.implementation.QueryParser.Query
import com.ramongilmoreno.datastore.v0.implementation.{Engine, QueryParser}
import org.scalatest._

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

class V0SQLTest extends AsyncFlatSpec {

  class NoOperationJDBCStatus extends JDBCStatus {

    def q(query: Query): Future[Either[(String, List[Any]), Exception]] = internalSQL(query)

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
    val result = new NoOperationJDBCStatus().q(parsed)
    val alias = Engine.tableAlias
    val aField = fieldValueName("a")
    val bField = fieldValueName("b")
    val cTable = tableName("c")
    val idField = recordIdName()
    val expiresField = recordExpiresName()
    result.flatMap {
      case Left(q) => Future {
        assert(q._1 == s"select $alias.$idField, $alias.$expiresField, $alias.$aField, $alias.$bField from $cTable as $alias where ($alias.$expiresField is null or $alias.$expiresField >= ?)")
      }
      case Right(exception) => throw exception
    }
  }

  it should "be able of doing an insert" in {
    val u = new NoOperationJDBCStatus().u(Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), new RecordMetadata()))
    val aTable = tableName("a")
    val bField = fieldValueName("b")
    val cField = fieldValueName("c")
    assert(u._2 == s"insert into $aTable ($bField, $cField, ${recordIdName()}) values (?, ?, ?)")
    assert(u._3 == Seq("1", "2", u._1))
  }

  it should "be able of doing an update" in {
    val recordMeta = new RecordMetadata()
    val id = "x"
    recordMeta.id = Some(id)
    val u = new NoOperationJDBCStatus().u(Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), recordMeta))
    val aTable = tableName("a")
    val bField = fieldValueName("b")
    val cField = fieldValueName("c")
    val idField = recordIdName()
    assert(u._2 == s"update $aTable set $bField = ?, $cField = ? where $idField = ?")
    assert(u._3 == Seq("1", "2", id))
  }
}
