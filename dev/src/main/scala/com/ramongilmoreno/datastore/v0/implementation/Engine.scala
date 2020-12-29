package com.ramongilmoreno.datastore.v0.implementation

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.QueryParser.Query

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

  def recordExpiresName(): FieldId = recordName("expires")

  def recordName(field: String): FieldId = s"record_$field"

  def result(rs: ResultSet, names: List[FieldId]): Either[Result, Exception] =
    try {
      @scala.annotation.tailrec
      def f(acc: List[Array[ValueType]]): List[Array[ValueType]] = {
        if (rs.next()) {
          f(names.map(rs.getString).toArray :: acc)
        } else {
          acc
        }
      }

      Left(Result(names, f(List()).reverse.toArray))
    } catch {
      case e: Exception => Right(e)
    }


  trait JDBCStatus {

    // Decimal deletes all trailing characters after not a number
    val integerRegex: Regex = "[^0-9].*$".r
    val decimalRegex: Regex = "^[0-9]*[^0-9]".r

    /**
     * Connection that will execute queries
     */
    def connection: Connection

    /**
     * Tells whether the given table exists
     */
    def tableExists(table: TableId)(implicit ec: ExecutionContext): Future[Either[Boolean, Exception]]

    /**
     * Tells existing columns in the table
     */
    def columnsExists(table: TableId, columns: Set[FieldId]): Future[Either[Set[(FieldId, Boolean)], Exception]]

    /**
     * Ensure columns exists
     */
    def makeColumnsExist(table: TableId, columns: Set[FieldId]): Future[Either[Unit, Exception]]

    /**
     * Query action
     */
    def query(q: Query)(implicit ec: ExecutionContext): Future[Either[Result, Exception]] = {
      internalSQL(q)
        .flatMap {
          case Left(query) => Future {
            try {
              val ps = connection.prepareStatement(query)
              result(ps.executeQuery(), q.fields)
            } catch {
              case e: Exception => Right(e)
            }
          }
          case Right(e) => Future(Right(e))
        }
    }

    protected def internalSQL(q: Query)(implicit ec: ExecutionContext): Future[Either[String, Exception]] = {
      columnsExists(q.table, q.fields.toSet).flatMap {
        case Left(fields) =>
          // Prepare query
          val f: String = fields.map(t => if (t._2) s"$tableAlias.${fieldValueName(t._1)}" else "\"\" as %s".format(fieldValueName(t._1))).mkString(", ")
          val c = q.condition match {
            case Some(c) => " where " + c.text(tableAlias)
            case None => ""
          }
          Future(Left(s"select $f from ${q.table} as $tableAlias$c"))
        case Right(e) => Future(Right(e))
      }
    }

    def update(records: List[Record])(implicit ec: ExecutionContext): Future[Either[List[RecordId], String]] = {
      def f(remaining: List[Record], acc: List[RecordId]): Future[Either[List[RecordId], String]] = {
        remaining match {
          case Nil => Future(Left(acc))
          case r :: rest =>
            val u = internalUpdate(r)
            Future {
              connection.prepareStatement(u._2)
            }
              .flatMap(ps => {
                (1 to u._3.length).zip(u._3).foreach(x => ps.setObject(x._1, x._2))
                Future {
                  ps.execute()
                }
                  .flatMap(if (_) f(rest, acc :+ u._1) else Future(Right(
                    s"""
                       |Failed to update
                       |$u
                       |""".stripMargin
                  )))
              })
        }
      }

      f(records, Nil)
    }

    protected def internalUpdate(record: Record): (RecordId, String, Seq[Any]) = {
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
      val id: (FieldId, RecordId) = (recordIdName(), record.meta.id.getOrElse(UUID.randomUUID().toString))
      val allAndId = all :+ id
      val q: (String, Seq[Any]) = record.meta.id match {
        case None =>
          // Insert
          val placeHolders = allAndId.map(_ => "?").mkString(", ")
          (s"insert into ? ($placeHolders) values ($placeHolders)", Seq(record.table) ++ allAndId.map(_._1) ++ allAndId.map(_._2))
        case Some(_) =>
          // Update
          val placeHolders = all.map(_ => "? = ?").mkString(", ")
          (s"update ? set $placeHolders where ? = ?", Seq(record.table) ++ allAndId.flatMap(i => Seq(i._1, i._2)))
      }

      // Completed; return statement and arguments list
      (id._2, q._1, q._2)
    }

    def integer(value: ValueType): Int = integerRegex.replaceFirstIn(value, "").toInt

    def decimal(value: ValueType): Int = decimalRegex.replaceFirstIn(value, "").toInt
  }

  case class Result(columns: List[FieldId], rows: Array[Array[ValueType]]) {
    def count(): Int = columns.length

    def value(row: Int, column: FieldId): ValueType = rows(row)(columns.indexOf(column))
  }

}
