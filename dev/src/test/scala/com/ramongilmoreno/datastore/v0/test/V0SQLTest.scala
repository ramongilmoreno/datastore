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

    def q(query: Query): Future[Either[Throwable, (String, List[Any])]] = internalSQL(query)

    def u(record: Record): (RecordId, String, Seq[Any]) = internalUpdate(record)

    override def tableExists(table: TableId)(implicit ec: ExecutionContext): Future[Either[Throwable, Boolean]] = Future(Right(true))(ec)

    override def columnsExists(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Throwable, Set[(FieldId, Boolean)]]] = Future {
      Right(columns.map((_, true)))
    }(ec)

    override def makeColumnsExist(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Throwable, Unit]] = throw new UnsupportedOperationException

    override def connection: Connection = throw new UnsupportedOperationException

    override def makeTableExist(table: TableId)(implicit ec: ExecutionContext): Future[Either[Throwable, Unit]] = throw new UnsupportedOperationException
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
      case Right(q) => Future {
        assert(q._1 == s"select $alias.$idField, $alias.$expiresField, $alias.$aField, $alias.$bField from $cTable as $alias where ($alias.$expiresField is null or $alias.$expiresField >= ?)")
      }
      case Left(exception) => throw exception
    }
  }

  it should "be able of doing an insert" in {
    val u = new NoOperationJDBCStatus().u(Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), RecordMetadata()))
    val aTable = tableName("a")
    val bField = fieldValueName("b")
    val cField = fieldValueName("c")
    assert(u._2 == s"insert into $aTable ($bField, $cField, ${recordActiveName()}, ${recordIdName()}) values (?, ?, ?, ?)")
    assert(u._3 == Seq("1", "2", recordActiveValueTrue, u._1))
  }

  it should "be able of doing an update" in {
    val id = "x"
    val recordMeta = RecordMetadata(Some(id))
    val u = new NoOperationJDBCStatus().u(Record("a", Map("b" -> FieldData("1"), "c" -> FieldData("2")), recordMeta))
    val aTable = tableName("a")
    val bField = fieldValueName("b")
    val cField = fieldValueName("c")
    val idField = recordIdName()
    val activeField = recordActiveName()
    assert(u._2 == s"update $aTable set $bField = ?, $cField = ?, $activeField = ? where $idField = ?")
    assert(u._3 == Seq("1", "2", recordActiveValueTrue, id))
  }
}
