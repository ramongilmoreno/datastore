package com.ramongilmoreno.datastore.v0

object API {

  type Values[K, T] = Map[K, T]

  type Id = String
  type RecordId = Id
  type FieldId = Id
  type TableId = Id
  type ValueType = String
  type ValuesActual = Values[FieldId, ValueType]

  type Timestamp = Long
  type TransactionId = Id

  case class RecordMetadata(id: Option[RecordId] = None, expires: Option[Timestamp] = None, active: Boolean = true)

  case class FieldMetadata(isInteger: Boolean = false, isDecimal: Boolean = false)

  case class FieldData(value: ValueType, meta: FieldMetadata = FieldMetadata())

  case class Record(table: TableId, data: Values[FieldId, FieldData], meta: RecordMetadata = RecordMetadata()) {}

  class Request(val depends: Option[TransactionId]) {}

  class Update(depends: Option[TransactionId], val updates: Seq[Record]) extends Request(depends) {}

  class Query(depends: Option[TransactionId], val query: String) extends Request(depends) {}

  class ResponseGood(val transaction: TransactionId) {}

  class ResponseBad(val reason: String) {}

  class UpdateGood(transaction: TransactionId, val updated: Seq[RecordId]) extends ResponseGood(transaction) {}

  class UpdateBad(reason: String) extends ResponseBad(reason) {}

  class QueryBad(reason: String) extends ResponseBad(reason) {}

  class QueryResult(transaction: TransactionId, val results: List[Record], meta: Option[ValuesActual]) extends ResponseGood(transaction)

}
