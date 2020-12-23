package com.ramongilmoreno.datastore.v0

object API {

  type Values[K, T] = Map[K, T]

  type Id = String
  type FieldId = Id
  type ValueType = String
  type ValuesActual = Values[FieldId, ValueType]

  class FieldData(val value: ValueType, val meta: Option[ValuesActual])

  class Record(val data: Values[FieldId, FieldData], val meta: ValuesActual) {}

  type TransactionId = Id
  type TableId = Id
  type CustomId = Id

  class Request (val originatorId: CustomId, val depends: Option[TransactionId]) {}

  class Update(originatorId: CustomId, depends: Option[TransactionId], val updates: List[(TableId, Record)]) extends Request(originatorId, depends) {}

  class Query(originatorId: CustomId, depends: Option[TransactionId], val query: String) extends Request(originatorId, depends) {}

  class Response(val originatorId: CustomId, val transaction: TransactionId) {}

  class UpdateGood(originatorId: CustomId, transaction: TransactionId) extends Response(originatorId, transaction) {}
  class UpdateBad(originatorId: CustomId, transaction: TransactionId, val reason: String) extends Response(originatorId, transaction) {}

  class QueryBad(originatorId: CustomId, transaction: TransactionId, val reason: String) extends Response(originatorId, transaction) {}
  class QueryResult (originatorId: CustomId, transaction: TransactionId, val results: List[Record], meta: Option[ValuesActual]) extends Response(originatorId, transaction)

}
