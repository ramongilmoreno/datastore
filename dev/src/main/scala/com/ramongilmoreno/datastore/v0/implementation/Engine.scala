package com.ramongilmoreno.datastore.v0.implementation

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.QueryParser.{Field, FieldOrValue, Query, Value}

import java.sql.{Connection, ResultSet}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex

object Engine {

  val tableAlias: TableId = "t"

  def fieldValueName(field: String): FieldId = name(field, "field", "value")

  private def name(field: String, prefix: String, suffix: String): Id = s"${prefix}_${field}_$suffix"

  def fieldMetaIntName(field: String): FieldId = fieldMetaName(field, "int")

  def fieldMetaDecimalName(field: String): FieldId = fieldMetaName(field, "decimal")

  def fieldMetaName(field: String, suffix: String): FieldId = name(field, "meta", suffix)

  def recordIdName(): FieldId = recordName("id")

  def recordName(field: String): FieldId = s"record_$field"

  def recordExpiresName(): FieldId = recordName("expires")

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

    // Decimal deletes all trailing characters after not a number
    val integerRegex: Regex = "[^0-9].*$".r
    val decimalRegex: Regex = "^[0-9]*[^0-9]".r

    /**
     * Tells whether the given table exists
     */
    def tableExists(table: TableId)(implicit ec: ExecutionContext): Future[Boolean]

    /**
     * Tells whether the given table contains the given column
     */
    def columnExists(table: TableId, column: FieldId)(implicit ec: ExecutionContext): Future[Boolean]

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
        ps <- Future {
          connection.prepareStatement(query)
        };
        rs <- Future {
          ps.executeQuery()
        };
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
      Future.sequence(columns.map(columnExists(table, _))).map(columns.zip(_).map(x => (x._1, if (x._2) Field(x._1) else Value(""))))

    def update(table: TableId, record: Record)(implicit ec: ExecutionContext): Future[RecordId] = ???

    protected def internalUpdate(table: TableId, record: Record): (String, Seq[Any]) = {
      val values: Seq[(FieldId, Any)] = record.data.toSeq.flatMap(t => {
        val field = t._1
        val value = t._2.value
        val actualValue = Some((fieldValueName(field), value))
        val meta: FieldMetadata = t._2.meta
        val integerMeta = if (meta.isDecimal) Some((fieldMetaIntName(field), integer(value))) else None
        val decimalMeta = if (meta.isDecimal) Some((fieldMetaDecimalName(field), decimal(value))) else None
        Seq(actualValue, integerMeta, decimalMeta).flatten
      })
      val meta: Seq[(FieldId, Any)] = Seq(record.meta.expires).flatten.map((recordExpiresName(), _))
      val all = values ++ meta
      val id = (recordIdName(), record.meta.id.getOrElse(UUID.randomUUID().toString))
      val allAndId = all :+ id
      val q: (String, Seq[Any]) = record.meta.id match {
        case None =>
          // Insert
          val placeHolders = allAndId.map(_ => "?").mkString(", ")
          (s"insert into ? ($placeHolders) values ($placeHolders)", Seq(table) ++ allAndId.map(_._1) ++ allAndId.map(_._2))
        case Some(_) =>
          // Update
          val placeHolders = all.map(_ => "? = ?").mkString(", ")
          (s"update ? set $placeHolders where ? = ?", Seq(table) ++ allAndId.flatMap(i => Seq(i._1, i._2)))
      }

      // Completed; return statement and arguments list
      q
    }

    def integer(value: ValueType): Int = integerRegex.replaceFirstIn(value, "").toInt

    def decimal(value: ValueType): Int = decimalRegex.replaceFirstIn(value, "").toInt
  }

  case class Result(columns: List[FieldId], rows: Array[Array[ValueType]]) {
    def count(): Int = columns.length

    def value(row: Int, column: FieldId): ValueType = rows(row)(columns.indexOf(column))
  }

}
