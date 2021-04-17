package com.ramongilmoreno.datastore.v0.implementation

import com.ramongilmoreno.datastore.v0.API.{Record, RecordId, RecordMetadata}
import com.ramongilmoreno.datastore.v0.implementation.Engine.{InMemoryH2Status, Result, TransactionCondition, TransactionResult, flatMapRightWrapper}
import com.ramongilmoreno.datastore.v0.implementation.EngineManager.extension
import com.ramongilmoreno.datastore.v0.implementation.QueryParser.Query

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object EngineManager {
  val extension = ".transaction.json"

  def createManager(dir: Path)(implicit ec: ExecutionContext): Future[Either[Throwable, EngineManager]] = {
    val em = new EngineManager(dir)
    em.init().flatMapRight(_ => Future(Right(em)))
  }

}

class EngineManager(dir: Path) {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HHmm_ss")
  val numberFormat = new DecimalFormat("000000")
  var status: InMemoryH2Status = _

  /**
   * Load configuration from filesystem
   */
  def init()(implicit ec: ExecutionContext): Future[Either[Throwable, Unit]] =
    Future {
      // Obtain in result the list of paths
      val result = mutable.MutableList[(Path, List[Array[Byte]])]()
      Files.walkFileTree(dir, new FileVisitor[Path]() {
        override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = FileVisitResult.CONTINUE

        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          if (file.toString.endsWith(extension)) {
            val normalized = file.normalize()
            val r = (normalized, normalized.iterator().map(_.normalize()).map(_.toString).map(_.getBytes(StandardCharsets.UTF_8)).toList)
            result += r
          }
          FileVisitResult.CONTINUE
        }

        override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = FileVisitResult.TERMINATE

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
      })

      @tailrec
      def compareBytes(a: Array[Byte], b: Array[Byte], index: Int): Int = {
        if (a.length > index && b.length > index) {
          val r = a(index) - b(index)
          if (r != 0) {
            r
          } else {
            compareBytes(a, b, index + 1)
          }
        } else {
          a.length - b.length
        }
      }

      @tailrec
      def compare(a: List[Array[Byte]], b: List[Array[Byte]]): Int = {
        if (a.isEmpty && b.isEmpty) {
          0
        } else if (a.isEmpty) {
          1
        } else if (b.isEmpty) {
          -1
        } else {
          val r = compareBytes(a.head, b.head, 0)
          if (r != 0) {
            r
          } else {
            compare(a.tail, b.tail)
          }
        }
      }
      // Sorted list of paths is the result of this action
      result.sorted((x: (Path, List[Array[Byte]]), y: (Path, List[Array[Byte]])) => compare(x._2, y._2)).map(_._1)
    }
      .flatMap(sorted => {
        // Apply all records until finished
        status = new InMemoryH2Status

        // Iterator over individual items (to ensure record exists, by calling status.makeRecord
        def exhaust2(items: Seq[Record]): Future[Either[Throwable, Unit]] = {
          if (items.isEmpty) {
            Future(Right())
          } else {
            val record = items.head
            status.makeRecordExists(record.meta.id.get, record.table)
              .flatMapRight(_ => exhaust2(items.tail))
          }
        }

        // Iterator over paths
        def exhaust(items: Seq[Path]): Future[Either[Throwable, Unit]] = {
          if (items.isEmpty) {
            Future(Right())
          } else {
            Future {
              APIManager.loadRecords(items.head)
            }
              .flatMapRight(records => {
                exhaust2(records)
                  .flatMapRight(_ => status.update(records))
              })
              .flatMapRight(_ => exhaust(items.tail))
          }
        }

        exhaust(sorted)
      })

  def query(q: Query)(implicit ec: ExecutionContext): Future[Either[Throwable, Result]] = status.query(q)

  def update(records: List[Record])(implicit ec: ExecutionContext): Future[Either[Throwable, List[RecordId]]] = {
    status.update(records)
      .flatMapRight(ids => {
        location()
          .flatMapRight(path => {
            val z: Seq[Record] = records.zip(ids).map(i => {
              val (record, id) = i
              Record(record.table, record.data, RecordMetadata(Some(id), record.meta.expires))
            })
            APIManager.saveRecords(z, path).flatMap(_ => Future(Right(ids)))
          })
      })
  }

  /**
   * Find a suitable (non existing) file location for the given timestamp
   */
  def location()(implicit ec: ExecutionContext): Future[Either[Throwable, Path]] = {
    val ts = dateFormat.format(new Date())

    @tailrec
    def exhaust(index: Int): Either[Throwable, Path] = {
      if (index > 999999) {
        Left(new IllegalStateException(s"Could not find a free path at timestamp [$ts] in path [$dir]."))
      }
      val v = numberFormat.format(index)
      val s = s"$ts-$v$extension"
      val p = dir.resolve(s)
      if (Files.exists(p))
        exhaust(index + 1)
      else
        Right(p)
    }

    Future {
      exhaust(0)
    }
  }

  def checkTransactionConditions(conditions: Seq[TransactionCondition])(implicit ec: ExecutionContext): Future[Either[Throwable, TransactionResult]] =
    status.checkTransactionConditions(conditions)
}
