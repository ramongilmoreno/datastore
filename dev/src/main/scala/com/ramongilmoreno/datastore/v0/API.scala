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

  abstract class Metadata {
  }

  class RecordMetadata extends Metadata {
    var id: Option[RecordId] = None
    var expires: Option[Timestamp] = None
  }

  class FieldMetadata extends Metadata {
    var isInteger: Boolean = false
    var isDecimal: Boolean = false
  }

  case class FieldData(value: ValueType, meta: FieldMetadata = new FieldMetadata())

  case class Record(table: TableId, data: Values[FieldId, FieldData], meta: RecordMetadata = new RecordMetadata()) {}

  class Request(val depends: Option[TransactionId]) {}

  class Update(depends: Option[TransactionId], val updates: List[Record]) extends Request(depends) {}

  class Query(depends: Option[TransactionId], val query: String) extends Request(depends) {}

  class Response(val transaction: TransactionId) {}

  class UpdateGood(transaction: TransactionId) extends Response(transaction) {}

  class UpdateBad(transaction: TransactionId, val reason: String) extends Response(transaction) {}

  class QueryBad(transaction: TransactionId, val reason: String) extends Response(transaction) {}

  class QueryResult(transaction: TransactionId, val results: List[Record], meta: Option[ValuesActual]) extends Response(transaction)

}
