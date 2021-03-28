package com.ramongilmoreno.datastore.v0.implementation

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.QueryParser.{Condition, Field, FieldOrValue, Query, SingleCondition, TwoCondition, Value}

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.{Locale, UUID}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex

object Engine {

  val tableAlias: TableId = "t"

  def fieldMetaIntName(field: String): FieldId = fieldMetaName(field, "int")

  def fieldMetaDecimalName(field: String): FieldId = fieldMetaName(field, "decimal")

  private def fieldMetaName(field: String, suffix: String): FieldId = name(field, "meta", suffix)

  def tableName(table: TableId): String = s"table_$table".toUpperCase(Locale.US)

  def now(): Long = System.currentTimeMillis()

  def result(rs: ResultSet, names: List[FieldId]): Either[Exception, Result] =
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

      Right(Result(names, f(List()).reverse.toArray))
    } catch {
      case e: Exception => Left(e)
    }

  def recordIdName(): FieldId = recordName("id")

  def recordExpiresName(): FieldId = recordName("expires")

  private def recordName(field: String): FieldId = s"record_$field".toUpperCase(Locale.US)

  def fieldValueName(field: String): FieldId = name(field, "field", "value")

  private def name(field: String, prefix: String, suffix: String): Id = s"${prefix}_${field}_$suffix".toUpperCase(Locale.US)

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
    def tableExists(table: TableId)(implicit ec: ExecutionContext): Future[Either[Exception, Boolean]]

    /**
     * Tells existing columns in the table
     */
    def columnsExists(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Exception, Set[(FieldId, Boolean)]]]

    /**
     * Ensure table exists
     */
    def makeTableExist(table: TableId)(implicit ec: ExecutionContext): Future[Either[Exception, Unit]]

    /**
     * Ensure columns exists
     */
    def makeColumnsExist(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Exception, Unit]]

    /**
     * Convenience method to parse a query
     */
    def query(q: CharSequence)(implicit ec: ExecutionContext): Future[Either[Exception, Result]] =
      query(QueryParser.parse(q).get)(ec)

    /**
     * Query action
     */
    def query(q: Query)(implicit ec: ExecutionContext): Future[Either[Exception, Result]] = {
      tableExists(q.table)
        .flatMap {
          case Right(false) =>
            // Empty result if table does not exist
            Future(Right(Result(q.fields, Array.empty)))
          case Right(true) =>
            // Run query
            internalSQL(q)
              .flatMap {
                case Right(query) => Future {
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
                    case e: Exception => Left(e)
                  }
                }
                case Left(e) => Future(Left(e))
              }
          case Left(e) =>
            // Exception thrown...
            Future(Left(e))
        }
    }

    protected def internalSQL(q: Query)(implicit ec: ExecutionContext): Future[Either[Exception, WorkInProgress]] = {
      columnsExists(q.table, q.fields.toSet).flatMap {
        case Right(fields) =>
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
          Future(Right((s"select $tableAlias.$queryIdName, $tableAlias.$queryExpiresName, $f from $queryTableName as $tableAlias where ($tableAlias.$queryExpiresName is null or $tableAlias.$queryExpiresName >= ?)${c._1}", now() +: c._2)))
        case Left(e) => Future(Left(new IllegalStateException(s"Failed to check that columns exist [$q]", e)))
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

    def update(records: Seq[Record])(implicit ec: ExecutionContext): Future[Either[Exception, List[RecordId]]] =
      update(records.toList)

    def update(records: List[Record])(implicit ec: ExecutionContext): Future[Either[Exception, List[RecordId]]] = {
      def exhaust(remaining: List[Record], acc: List[RecordId]): Future[Either[Exception, List[RecordId]]] = remaining match {
        case Nil => Future(Right(acc))
        case r :: rest =>
          val u = internalUpdate(r)
          makeTableExist(r.table)
            .flatMap {
              case Right(_) => makeColumnsExist(r.table, r.data.keySet)
              case Left(exception) => Future(Left(new IllegalStateException(s"Unable to make table exists [$u]", exception)))
            }
            .flatMap {
              case Right(_) =>
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
                  case e: Exception => Future(Left(new IllegalStateException(s"Failed to update[$u]", e)))
                }
              case Left(exception) => Future(Left(new IllegalStateException(s"Unable to make columns exists [$u]", exception)))
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

    def makeRecordExists(id: Id, table: TableId)(implicit ec: ExecutionContext): Future[Either[Throwable, Unit]] = {
      makeTableExist(table)
        .flatMap {
          case Left(e) => Future(Left(e))
          case Right(_) => Future {
            val ps = connection.prepareStatement("select count(*) from " + tableName(table))
            try {
              val rs = ps.executeQuery()
              rs.next()
              Right(rs.getInt(1))
            } catch {
              case t: Throwable => Left(t)
            } finally {
              ps.close()
            }
          }
        }
        .flatMap {
          case Left(t) => Future(Left(t))
          case Right(count) =>
            if (count > 0) {
              // Record exists, nothing to be done
              Future(Right())
            } else {
              // Record does not exist, insert it
              Future {
                val queryTableName = tableName(table)
                val idFieldName = recordIdName()
                val q = s"insert into $queryTableName ($idFieldName) values (?)"
                try {
                  val ps = connection.prepareStatement(q)
                  try {
                    ps.setObject(1, id)
                    ps.execute()
                    Right()
                  } finally {
                    ps.close()
                  }
                } catch {
                  case e: Exception => Left[Throwable, Unit](new IllegalStateException(s"Insert threw an exception: [$q] for id [$id]", e))
                }
              }
            }
        }
    }

  }

  abstract class H2Status extends JDBCStatus {

    val basicType = "VARCHAR(500)"
    val basicNumberType = "BIGINT"

    /**
     * Ensure columns exists
     */
    override def makeColumnsExist(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Exception, Unit]] =
      columnsExists(table, columns)
        .flatMap {
          case Right(c) =>
            val missing: Set[FieldId] = c.filterNot(_._2).map(_._1)
            if (missing.nonEmpty) {
              val columns: List[(FieldId, String)] = missing.toList.flatMap(field => List(
                (fieldValueName(field), basicType),
                (fieldMetaIntName(field), basicNumberType),
                (fieldMetaDecimalName(field), basicNumberType)
              ))

              def exhaust(pending: List[(FieldId, String)]): Either[Exception, Unit] = pending match {
                case Nil => Right()
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
                    case e: Exception => Left(new IllegalStateException(s"Failed to add column [${column._1}] to table [$table]", e))
                  }
              }

              Future {
                exhaust(columns)
              }
            } else {
              Future(Right())
            }
          case Left(e) => Future(Left(new IllegalStateException(s"Failed to check if columns exists in table [$table], columns [$columns]", e)))
        }

    /**
     * Tells existing columns in the table
     */
    override def columnsExists(table: TableId, columns: Set[FieldId])(implicit ec: ExecutionContext): Future[Either[Exception, Set[(FieldId, Boolean)]]] =
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
            Right(columns.map(c => (c, found.contains(fieldValueName(c)))))
          } finally {
            ps.close()
          }
        } catch {
          case e: Exception => Left(new IllegalStateException(s"Failed to check table [$table] columns [$columns]", e))
        }
      }

    /**
     * Ensure table exists
     */
    override def makeTableExist(table: TableId)(implicit ec: ExecutionContext): Future[Either[Exception, Unit]] =
      tableExists(table).flatMap({
        case Right(true) =>
          // Do nothing
          Future(Right())
        case Right(false) =>
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
                Right()
              } finally {
                ps.close()
              }
            } catch {
              case e: Exception => Left(new IllegalStateException(s"Could not create table [$table]", e))
            }
          }
        case Left(e) => Future(Left(new IllegalStateException(s"Failed to check if table exists [$table]", e)))
      })

    /**
     * Tells whether the given table exists
     */
    override def tableExists(table: TableId)(implicit ec: ExecutionContext): Future[Either[Exception, Boolean]] =
      Future {
        try {
          // http://h2-database.66688.n3.nabble.com/Best-practice-Test-for-existence-of-table-etc-td4024451.html
          val ps = connection.prepareStatement("select count(*) from information_schema.tables where table_name = ?")
          try {
            ps.setString(1, tableName(table))
            val rs = ps.executeQuery()
            rs.next()
            if (rs.getInt(1) == 1) Right(true) else Right(false)
          } finally {
            ps.close()
          }
        } catch {
          case e: Exception => Left(e)
        }
      }
  }

  case class Result(columns: List[FieldId], rows: Array[(Array[ValueType], RecordMetadata)]) {
    def count(): Int = rows.length

    def meta(row: Int): RecordMetadata = rows(row)._2

    def value(row: Int, column: FieldId): ValueType = rows(row)._1(columns.indexOf(column))
  }

  class InMemoryH2Status extends H2Status {
    Class.forName("org.h2.Driver")
    private val url = "jdbc:h2:mem:" + UUID.randomUUID().toString
    private val conn = DriverManager.getConnection(url)

    override def connection: Connection = conn
  }

}
