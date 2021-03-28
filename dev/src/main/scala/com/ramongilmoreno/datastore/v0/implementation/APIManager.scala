package com.ramongilmoreno.datastore.v0.implementation

import com.ramongilmoreno.datastore.v0.API._
import spray.json.{JsArray, JsBoolean, JsObject, JsString, JsValue, JsonParser}

import java.io.{InputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Codec

/**
 * Makes API actions easier for EngineManager. Mostly JSON conversion.
 */
object APIManager {

  val TABLE_FIELD = "table"
  val DATA_FIELD_PREFIX = "field_"
  val META_FIELD = "meta"
  val VALUE_FIELD = "value"
  val IS_INTEGER_FIELD = "isInteger"
  val IS_DECIMAL_FIELD = "isDecimal"
  val ID_FIELD = "id"
  val EXPIRES_FIELD = "expires"

  def loadRecords(input: Path)(implicit ec: ExecutionContext): Either[Throwable, Seq[Record]] =
    try {
      val is = Files.newInputStream(input)
      val r = loadRecords(is)
      is.close()
      r
    } catch {
      case e: Throwable => Left(e)
    }

  def loadRecords(input: InputStream)(implicit ec: ExecutionContext): Either[Throwable, Seq[Record]] =
    try {
      val string = scala.io.Source.fromInputStream(input)(Codec.UTF8).mkString
      Right(JsonParser(string).asInstanceOf[JsArray].elements.map(x => loadRecord(x.asInstanceOf[JsObject])))
    } catch {
      case e: Throwable => Left(e)
    }

  def loadRecord(input: JsObject): Record = {
    val fields = input.fields
    val table = fields(TABLE_FIELD).asInstanceOf[JsString].value
    val data = fields.filter(_._1.startsWith(DATA_FIELD_PREFIX)).map(tuple => (tuple._1.substring(DATA_FIELD_PREFIX.length), loadFieldData(tuple._2.asJsObject)))
    val meta = loadRecordMetadata(fields(META_FIELD).asJsObject)
    Record(table, data, meta)
  }

  def loadFieldData(input: JsObject): FieldData = {
    val fields = input.fields
    val value = fields(VALUE_FIELD).asInstanceOf[JsString].value
    val meta = loadFieldMetadata(fields(META_FIELD).asJsObject)
    FieldData(value, meta)
  }

  def loadFieldMetadata(input: JsObject): FieldMetadata = {
    val fields = input.fields
    val isInteger: Boolean = fields(IS_DECIMAL_FIELD).asInstanceOf[JsBoolean].value
    val isDecimal: Boolean = fields(IS_DECIMAL_FIELD).asInstanceOf[JsBoolean].value
    FieldMetadata(isInteger, isDecimal)
  }

  def loadRecordMetadata(input: JsObject): RecordMetadata = {
    val fields = input.fields
    val id: Option[String] = fields.get(ID_FIELD).map(_.asInstanceOf[JsString]).map(_.value)
    val expires: Option[Timestamp] = fields.get(EXPIRES_FIELD).map(_.asInstanceOf[JsString]).map(_.value).map(_.toLong)
    RecordMetadata(id, expires)
  }

  def saveRecords(input: Seq[Record], output: Path)(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      Files.newOutputStream(output)
    }
      .flatMap(os => {
        saveRecords(input, os).flatMap(_ => Future {
          os.close()
        })
      })

  def saveRecords(input: Seq[Record], output: OutputStream)(implicit ec: ExecutionContext): Future[Unit] = Future {
    val js = new JsArray(input.map(saveRecord).toVector)
    val oos = new OutputStreamWriter(output, StandardCharsets.UTF_8.name())
    oos.write(js.prettyPrint)
    oos.flush()
  }

  def saveRecord(input: Record): JsObject = {
    val table = TABLE_FIELD -> JsString(input.table)
    val data: Map[String, JsValue] = input.data.map(tuple => (DATA_FIELD_PREFIX + tuple._1, saveFieldData(tuple._2)))
    val meta = META_FIELD -> saveRecordMetadata(input.meta)
    JsObject(data + table + meta)
  }

  def saveRecordMetadata(input: RecordMetadata): JsValue = {
    val id: Map[String, JsValue] = input.id.map(JsString(_)).map(v => Map[String, JsValue](ID_FIELD -> v)).getOrElse(Map.empty)
    val expires: Map[String, JsValue] = input.expires.map(v => JsString(v.toString)).map(v => Map[String, JsValue](EXPIRES_FIELD -> v.asInstanceOf[JsValue])).getOrElse(Map.empty)
    JsObject(Map.empty[String, JsValue] ++ id ++ expires)
  }

  def saveFieldData(input: FieldData): JsValue =
    JsObject(Map(VALUE_FIELD -> JsString(input.value), META_FIELD -> saveFieldMetadata(input.meta)))

  def saveFieldMetadata(input: FieldMetadata): JsValue =
    JsObject(Map(IS_INTEGER_FIELD -> JsBoolean(input.isInteger), IS_DECIMAL_FIELD -> JsBoolean(input.isDecimal)))
}
