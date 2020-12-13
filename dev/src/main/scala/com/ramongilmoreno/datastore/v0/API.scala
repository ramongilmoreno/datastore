package com.ramongilmoreno.datastore.v0

object API {

  type Values[K, T] = Map[K, T]

  type FieldId = String
  type FieldValue = String
  type ValuesActual = Values[FieldId, FieldValue]

  class FieldData(val value: FieldValue, val meta: Option[ValuesActual])

  class Record(val data: Values[FieldId, FieldData], val meta: ValuesActual) {}

  type TransactionId = String
  type TableId = String
  type CustomId = String

  class Request (val originatorId: CustomId, val depends: Option[TransactionId]) {}

  class Update(originatorId: CustomId, depends: Option[TransactionId], val updates: List[(TableId, Record)]) extends Request(originatorId, depends) {}

  class Query(originatorId: CustomId, depends: Option[TransactionId], val query: String) extends Request(originatorId, depends) {}

  class Response(val originatorId: CustomId, val transaction: TransactionId) {}

  class UpdateGood(originatorId: CustomId, transaction: TransactionId) extends Response(originatorId, transaction) {}
  class UpdateBad(originatorId: CustomId, transaction: TransactionId, val reason: String) extends Response(originatorId, transaction) {}

  class QueryBad(originatorId: CustomId, transaction: TransactionId, val reason: String) extends Response(originatorId, transaction) {}
  class QueryResult (originatorId: CustomId, transaction: TransactionId, val results: List[Record], meta: Option[ValuesActual]) extends Response(originatorId, transaction)

}
