package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.{EngineManager, QueryParser}
import org.scalatest._

import java.nio.file.Path
import scala.concurrent.Future

class EngineManagerTest extends AsyncFlatSpec {

  import com.google.common.jimfs.{Configuration, Jimfs}

  import java.nio.file.FileSystem

  // https://stackoverflow.com/a/31178425
  val configuration: Configuration = Configuration.unix().toBuilder.setWorkingDirectory("/").build()
  val fs: FileSystem = Jimfs.newFileSystem(configuration)
  val path: Path = fs.getPath("/")

  def sampleRecordData(value: String, value2: String = "x"): Record = {
    val meta = RecordMetadata()
    Record("a", Map("b" -> FieldData(value), "c" -> FieldData(value2)), meta)
  }

  "Engine manager" should "be able of doing an insert and recover status" in {
    EngineManager.createManager(path)
      .flatMap {
        case Left(e) => Future(Left(e))
        case Right(manager) => {
          manager
            .update(List(sampleRecordData("1", "2")))
        }
      }
      .flatMap {
        case Left(e) => Future(Left(e))
        case Right(_) => EngineManager.createManager(path)
      }
      .flatMap {
        case Left(e) => Future(Left(e))
        case Right(manager) => {
          /* Newly created manager */ manager.query(QueryParser.parse("select b, c from a").get)
        }
      }
      .flatMap {
        case Left(e) => fail(e)
        case Right(result) => {
          assert(result.rows.length == 1)
          assert(result.value(0, "b") == "1")
          assert(result.value(0, "c") == "2")
        }
      }
  }
}