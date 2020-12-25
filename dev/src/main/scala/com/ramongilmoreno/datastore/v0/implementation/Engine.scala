package com.ramongilmoreno.datastore.v0.implementation

import java.sql.{Connection, ResultSet}
import com.ramongilmoreno.datastore.v0.API.{FieldId, Id, TableId, ValueType}
import com.ramongilmoreno.datastore.v0.implementation.QueryParser.{Field, FieldOrValue, Query, Value}

import scala.concurrent.{ExecutionContext, Future}

object Engine {

  val tableAlias: TableId = "t"

  private def name (field: String, prefix: String, suffix: String): Id = s"${prefix}_${field}_$suffix"
  def fieldValueName (field: String): FieldId = name(field, "field", "value")
  def fieldMetaName (field: String, suffix: String): FieldId = name(field, "meta", s"meta_$suffix")
  def fieldMetaIdName (field: String): FieldId = fieldMetaName(field, "id")
  def fieldMetaIntName (field: String): FieldId = fieldMetaName(field, "int")
  def fieldMetaDecimal1Name (field: String): FieldId = fieldMetaName(field, "decimal1")
  def fieldMetaDecimal2Name (field: String): FieldId = fieldMetaName(field, "decimal2")

  def result(rs: ResultSet, names: List[FieldId])(implicit ec: ExecutionContext): Future[Result] = Future {
    @scala.annotation.tailrec
    def f(acc: List[Array[ValueType]]): List[Array[ValueType]] = {
      if (rs.next()) {
        f(names.map(rs.getString).toArray :: acc)
      } else {
        acc
      }
    }

    Result(names, f(List()).reverse.toArray)
  }

  trait JDBCStatus {

    /**
     * Tells whether the given table contains the given column
     */
    def column(table: TableId, column: FieldId)(implicit ec: ExecutionContext): Future[Boolean]

    /**
     * Connection that will execute queries
     */
    def connection(): Future[Connection]

    /**
     * Query action
     */
    def query(q: Query)(implicit ec: ExecutionContext): Future[Result] = {
      for (
        query <- internalSQL(q);
        connection <- connection();
        ps <-  Future { connection.prepareStatement(query) };
        rs <- Future { ps.executeQuery() };
        r <- result(rs, q.fields)
      ) yield {
        rs.close()
        r
      }
    }

    protected def internalSQL(q: Query)(implicit ec: ExecutionContext): Future[String] = {
      val table = q.table
      existingColumns(q.table, q.fields).flatMap(
        fields => {
          // Prepare query
          val f = fields.map(t => t._2 match {
            case Field(f) => s"$tableAlias.${fieldValueName(f)}"
            case Value(v) => "\"" + v + "\" as " + fieldValueName(t._1)
          }).mkString(", ")
          val c = q.condition match {
            case Some(c) => " where " + c.text(tableAlias)
            case None => ""
          }
          Future(s"select $f from ${q.table} as $tableAlias")
        }
      )
    }

    def existingColumns(table: TableId, columns: List[FieldId])(implicit ec: ExecutionContext): Future[List[(FieldId, FieldOrValue)]] =
      Future.sequence(columns.map(column(table, _))).map(columns.zip(_).map(x => (x._1, if (x._2) Field(x._1) else Value(""))))
  }

  case class Result(columns: List[FieldId], rows: Array[Array[ValueType]]) {
    def count(): Int = columns.length

    def value(row: Int, column: FieldId): ValueType = rows(row)(columns.indexOf(column))
  }

}
