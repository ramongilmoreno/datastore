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

  abstract class Metadata {
    var expires: Option[Timestamp] = None
  }

  class RecordMetadata extends Metadata {
    var id: Option[RecordId] = None
  }

  class FieldMetadata extends Metadata {
    val isInteger: Boolean = false
    val isDecimal: Boolean = false
  }

  class FieldData(val value: ValueType, val meta: FieldMetadata)

  class Record(val data: Values[FieldId, FieldData], val meta: RecordMetadata) {}

  type TransactionId = Id

  class Request (val depends: Option[TransactionId]) {}

  class Update(depends: Option[TransactionId], val updates: List[(TableId, Record)]) extends Request(depends) {}

  class Query(depends: Option[TransactionId], val query: String) extends Request(depends) {}

  class Response(val transaction: TransactionId) {}

  class UpdateGood(transaction: TransactionId) extends Response(transaction) {}
  class UpdateBad(transaction: TransactionId, val reason: String) extends Response(transaction) {}

  class QueryBad(transaction: TransactionId, val reason: String) extends Response(transaction) {}
  class QueryResult (transaction: TransactionId, val results: List[Record], meta: Option[ValuesActual]) extends Response(transaction)

}
