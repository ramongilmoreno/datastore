package com.ramongilmoreno.datastore.v0.implementation

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.QueryParser.{Condition, Field, FieldOrValue, Query, SingleCondition, TwoCondition, Value}

import java.sql.{Connection, ResultSet}
import java.util.{Locale, UUID}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex

object Engine {

  val tableAlias: TableId = "t"

  def fieldMetaIntName(field: String): FieldId = fieldMetaName(field, "int")

  private def fieldMetaName(field: String, suffix: String): FieldId = name(field, "meta", suffix)

  private def name(field: String, prefix: String, suffix: String): Id = s"${prefix}_${field}_$suffix".toUpperCase(Locale.US)

  def fieldMetaDecimalName(field: String): FieldId = fieldMetaName(field, "decimal")

  def tableName(table: TableId): String = s"table_$table".toUpperCase(Locale.US)

  def now(): Long = System.currentTimeMillis()

  def result(rs: ResultSet, names: List[FieldId]): Either[Result, Exception] =
    try {
      @scala.annotation.tailrec
      def f(acc: List[(Array[ValueType], RecordMetadata)]): List[(Array[ValueType], RecordMetadata)] = {
        if (rs.next()) {
          val id: RecordId = rs.getString(recordIdName())
          val o = rs.getLong(recordExpiresName())
          val expires: Option[Timestamp] = if (rs.wasNull()) None else Some(o)
          val meta = RecordMetadata(Some(id), expires)
          f((names.map(field => rs.getString(fieldValueName(field))).toArray, meta) :: acc)
        } else {
          acc
        }
      }

      Left(Result(names, f(List()).reverse.toArray))
    } catch {
      case e: Exception => Right(e)
    }

  def recordIdName(): FieldId = recordName("id")

  private def recordName(field: String): FieldId = s"record_$field".toUpperCase(Locale.US)

  def recordExpiresName(): FieldId = recordName("expires")

  def fieldValueName(field: String): FieldId = name(field, "field", "value")

  trait JDBCStatus {

    type WorkInProgress = (String, List[Any])
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
    def columnsExists(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Set[(FieldId, Boolean)], Exception]]

    /**
     * Ensure table exists
     */
    def makeTableExist(table: TableId)(implicit ec: ExecutionContext): Future[Either[Unit, Exception]]

    /**
     * Ensure columns exists
     */
    def makeColumnsExist(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Unit, Exception]]

    /**
     * Query action
     */
    def query(q: Query)(implicit ec: ExecutionContext): Future[Either[Result, Exception]] = {
      tableExists(q.table)
        .flatMap {
          case Left(false) =>
            // Empty result if table does not exist
            Future(Left(Result(q.fields, Array.empty)))
          case Left(true) =>
            // Run query
            internalSQL(q)
              .flatMap {
                case Left(query) => Future {
                  try {
                    val ps = connection.prepareStatement(query._1)
                    try {
                      val args = query._2
                      (1 to args.length).zip(args).foreach(f => ps.setObject(f._1, f._2))
                      result(ps.executeQuery(), q.fields)
                    } finally {
                      ps.close()
                    }
                  } catch {
                    case e: Exception => Right(e)
                  }
                }
                case Right(e) => Future(Right(e))
              }
          case Right(e) =>
            // Exception thrown...
            Future(Right(e))
        }
    }

    protected def internalSQL(q: Query)(implicit ec: ExecutionContext): Future[Either[WorkInProgress, Exception]] = {
      columnsExists(q.table, q.fields.toSet).flatMap {
        case Left(fields) =>
          // Prepare query
          val f: String = fields.map(t => if (t._2) s"$tableAlias.${fieldValueName(t._1)}" else "\"\" as %s".format(fieldValueName(t._1))).mkString(", ")
          val c = q.condition match {
            case Some(c) =>
              val r = internalCondition(c)
              (s" and ${r._1}", r._2)
            case None => ("", List.empty)
          }
          val queryTableName = tableName(q.table)
          val queryIdName = recordIdName()
          val queryExpiresName = recordExpiresName()
          Future(Left((s"select $tableAlias.$queryIdName, $tableAlias.$queryExpiresName, $f from $queryTableName as $tableAlias where ($tableAlias.$queryExpiresName is null or $tableAlias.$queryExpiresName >= ?)${c._1}", now() +: c._2)))
        case Right(e) => Future(Right(new IllegalStateException(s"Failed to check that columns exist [$q]", e)))
      }
    }

    protected def internalCondition(c: Condition): WorkInProgress = {

      def fov(arg: FieldOrValue, acc: WorkInProgress): WorkInProgress = arg match {
        case field: Field =>
          val fname = fieldValueName(field.id)
          (s"${acc._1}$tableAlias.$fname", acc._2)
        case value: Value =>
          (acc._1 + "?", acc._2 :+ value.value)
      }

      def f(c: Condition, acc: WorkInProgress): WorkInProgress = c match {
        case two: TwoCondition =>
          val l = f(two.left, (acc._1 + "(", acc._2))
          val r = f(two.right, (s"${l._1} ${two.operator} ", l._2))
          (r._1 + ")", r._2)
        case single: SingleCondition =>
          val l = fov(single.left, (acc._1 + "(", acc._2))
          val r = fov(single.right, (s"${l._1} ${single.operator} ", l._2))
          (r._1 + ")", r._2)
      }

      f(c, ("", List.empty))
    }

    def update(records: List[Record])(implicit ec: ExecutionContext): Future[Either[List[RecordId], Exception]] = {
      def exhaust(remaining: List[Record], acc: List[RecordId]): Future[Either[List[RecordId], Exception]] = remaining match {
        case Nil => Future(Left(acc))
        case r :: rest =>
          val u = internalUpdate(r)
          makeTableExist(r.table)
            .flatMap {
              case Left(_) => makeColumnsExist(r.table, r.data.keySet)
              case Right(exception) => Future(Right(new IllegalStateException(s"Unable to make table exists [$u]", exception)))
            }
            .flatMap {
              case Left(_) =>
                try {
                  val ps = connection.prepareStatement(u._2)
                  try {
                    (1 to u._3.length).zip(u._3).foreach(x => ps.setObject(x._1, x._2))
                    ps.execute()
                    exhaust(rest, u._1 :: acc)
                  } finally {
                    ps.close()
                  }
                } catch {
                  case e: Exception => Future(Right(new IllegalStateException(s"Failed to update[$u]", e)))
                }
              case Right(exception) => Future(Right(new IllegalStateException(s"Unable to make columns exists [$u]", exception)))
            }

      }

      exhaust(records, Nil)
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
      val queryTableName = tableName(record.table)
      val q: (String, Seq[Any]) = record.meta.id match {
        case None =>
          // Insert
          val columns = allAndId.map(_._1).mkString(", ")
          val placeHolders = allAndId.map(_ => "?").mkString(", ")
          (s"insert into $queryTableName ($columns) values ($placeHolders)", allAndId.map(_._2))
        case Some(_) =>
          // Update
          val placeHolders = all.map(f => s"${f._1} = ?").mkString(", ")
          val queryIdField = recordIdName()
          (s"update $queryTableName set $placeHolders where $queryIdField = ?", allAndId.map(_._2))
      }

      // Completed; return statement and arguments list
      (id._2, q._1, q._2)
    }

    def integer(value: ValueType): Int = integerRegex.replaceFirstIn(value, "").toInt

    def decimal(value: ValueType): Int = decimalRegex.replaceFirstIn(value, "").toInt
  }

  abstract class H2Status extends JDBCStatus {

    val basicType = "VARCHAR(500)"
    val basicNumberType = "BIGINT"

    /**
     * Ensure columns exists
     */
    override def makeColumnsExist(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Unit, Exception]] =
      columnsExists(table, columns)
        .flatMap {
          case Left(c) =>
            val missing: Set[FieldId] = c.filterNot(_._2).map(_._1)
            if (missing.nonEmpty) {
              val columns: List[(FieldId, String)] = missing.toList.flatMap(field => List(
                (fieldValueName(field), basicType),
                (fieldMetaIntName(field), basicNumberType),
                (fieldMetaDecimalName(field), basicNumberType)
              ))

              def exhaust(pending: List[(FieldId, String)]): Either[Unit, Exception] = pending match {
                case Nil => Left()
                case column :: rest =>
                  val queryTableName = tableName(table)
                  val queryColumnName = column._1
                  val queryType = column._2
                  val q = s"alter table $queryTableName add column $queryColumnName $queryType"
                  try {
                    val ps = connection.prepareStatement(q)
                    try {
                      ps.execute()
                      exhaust(rest)
                    } finally {
                      ps.close()
                    }
                  } catch {
                    case e: Exception => Right(new IllegalStateException(s"Failed to add column [${column._1}] to table [$table]", e))
                  }
              }

              Future {
                exhaust(columns)
              }
            } else {
              Future(Left())
            }
          case Right(e) => Future(Right(new IllegalStateException(s"Failed to check if columns exists in table [$table], columns [$columns]", e)))
        }

    /**
     * Tells existing columns in the table
     */
    override def columnsExists(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Set[(FieldId, Boolean)], Exception]] =
      Future {
        try {
          // http://h2-database.66688.n3.nabble.com/Best-practice-Test-for-existence-of-table-etc-td4024451.html
          val ps = connection.prepareStatement("select column_name from information_schema.columns where table_name = ?")
          try {
            ps.setString(1, tableName(table))
            val rs = ps.executeQuery()

            @tailrec
            def exhaust(acc: Set[String]): Set[String] =
              if (rs.next()) {
                exhaust(acc + rs.getString(1))
              } else {
                acc
              }

            val found = exhaust(Set.empty)
            Left(columns.map(c => (c, found.contains(fieldValueName(c)))))
          } finally {
            ps.close()
          }
        } catch {
          case e: Exception => Right(new IllegalStateException(s"Failed to check table [$table] columns [$columns]", e))
        }
      }

    /**
     * Ensure table exists
     */
    override def makeTableExist(table: TableId)(implicit ec: ExecutionContext): Future[Either[Unit, Exception]] =
      tableExists(table).flatMap({
        case Left(true) =>
          // Do nothing
          Future(Left())
        case Left(false) =>
          // Create table
          Future {
            try {
              val queryTableName = tableName(table)
              val queryIdFieldName = recordIdName()
              val queryExpiresFieldName = recordExpiresName()
              val q = s"create table $queryTableName ($queryIdFieldName $basicType PRIMARY KEY, $queryExpiresFieldName $basicNumberType)"
              val ps = connection.prepareStatement(q)
              try {
                ps.execute()
                Left()
              } finally {
                ps.close()
              }
            } catch {
              case e: Exception => Right(new IllegalStateException(s"Could not create table [$table]", e))
            }
          }
        case Right(e) => Future(Right(new IllegalStateException(s"Failed to check if table exists [$table]", e)))
      })

    /**
     * Tells whether the given table exists
     */
    override def tableExists(table: TableId)(implicit ec: ExecutionContext): Future[Either[Boolean, Exception]] =
      Future {
        try {
          // http://h2-database.66688.n3.nabble.com/Best-practice-Test-for-existence-of-table-etc-td4024451.html
          val ps = connection.prepareStatement("select count(*) from information_schema.tables where table_name = ?")
          try {
            ps.setString(1, tableName(table))
            val rs = ps.executeQuery()
            rs.next()
            if (rs.getInt(1) == 1) Left(true) else Left(false)
          } finally {
            ps.close()
          }
        } catch {
          case e: Exception => Right(e)
        }
      }
  }

  case class Result(columns: List[FieldId], rows: Array[(Array[ValueType], RecordMetadata)]) {
    def count(): Int = rows.length

    def meta(row: Int): RecordMetadata = rows(row)._2

    def value(row: Int, column: FieldId): ValueType = rows(row)._1(columns.indexOf(column))
  }

}
