package com.ramongilmoreno.datastore.v0.test

import com.ramongilmoreno.datastore.v0.API._
import com.ramongilmoreno.datastore.v0.implementation.APIManager
import org.scalatest._
import spray.json.JsonParser

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.concurrent.Future

class V0APIManagerTest extends AsyncFlatSpec {

  def record(table: String, index: Int, field: String, value: ValueType): Record = {
    val field1 = "A"
    val value1 = FieldData("1", FieldMetadata(true, true))
    val field2 = "B"
    val value2 = FieldData("2")
    Record(table, Map(field1 -> value1, field2 -> value2, field -> FieldData(value)), RecordMetadata(Some(index.toString)))
  }

  "API manager" should "save a record to a file and read the same values" in {
    val original = record("T", 0, "other", "X")
    val jsonText = APIManager.saveRecord(original).prettyPrint
    val jsonObject = JsonParser(jsonText).asJsObject
    val read = APIManager.loadRecord(jsonObject)
    assert(original == read)
  }

  it should "be able to save and read a list of records" in {
    val v1 = record("T", 0, "other", "X")
    val v2 = record("U", 1, "another", "Y")
    val original = Seq(v1, v2)
    val baos = new ByteArrayOutputStream()
    APIManager.saveRecords(original, baos)
      .flatMap(_ => {
        baos.flush()
        Future {
          APIManager.loadRecords(new ByteArrayInputStream(baos.toByteArray)) match {
            case Left(e: Throwable) => fail(e)
            case Right(read) => assert(original == read)
          }
        }
      })
  }
}