package com.ramongilmoreno.datastore.v0.test

import java.sql.Connection

import com.ramongilmoreno.datastore.v0.API.{FieldId, TableId}
import com.ramongilmoreno.datastore.v0.implementation.Engine.JDBCStatus
import com.ramongilmoreno.datastore.v0.implementation.{Engine, QueryParser}
import org.scalatest._

import scala.concurrent.{ExecutionContext, Future}

class V0EngineTest extends AsyncFlatSpec {

  "Engine" should "get simple SQL for a query without conditions" in {
    val query = "select a, b from c"
    val parsed = QueryParser.parse(query).get
    val result: Future[String] = new JDBCStatus {
      override def column(table: TableId, column: FieldId)(implicit ec: ExecutionContext): Future[Boolean] = {
        Future(true)(ec)
      }
      override def connection(): Future[Connection] = throw new UnsupportedOperationException
      def q(): Future[String] = internalSQL(parsed)
    }.q()
    val alias = Engine.tableAlias
    result.flatMap(sql => assert(sql == s"select $alias.a as a, $alias.b as b from c as $alias"))
  }
}
