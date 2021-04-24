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

  trait Result {
    def fieldsCount(): Int

    def field(position: Int): FieldId

    def count(): Int

    def meta(row: Int): RecordMetadata

    def value(row: Int, column: FieldId): ValueType
  }

  trait QueryResponse

  trait UpdateResponse

  trait TransactionResponse

  case class RecordMetadata(id: Option[RecordId] = None, expires: Option[Timestamp] = None, active: Boolean = true)

  case class FieldMetadata(isInteger: Boolean = false, isDecimal: Boolean = false)

  case class FieldData(value: ValueType, meta: FieldMetadata = FieldMetadata())

  case class Record(table: TableId, data: Values[FieldId, FieldData], meta: RecordMetadata = RecordMetadata()) {}

  case class QueryRequest(depends: Seq[TransactionId], query: String)

  case class QueryResult(result: Result) extends QueryResponse

  case class QueryFailed(throwable: Throwable) extends QueryResponse

  case class UpdateRequest(depends: Seq[TransactionId], updates: Seq[Record])

  case class UpdateGood(transaction: TransactionId, ids: Seq[RecordId]) extends UpdateResponse

  case class UpdateBad(throwable: Throwable) extends UpdateResponse

  case class TransactionSingleCondition(query: String, expected: Seq[Record])

  case class TransactionSingleConditionResult(ok: Boolean, found: Result)

  case class TransactionRequest(depends: Seq[TransactionId], conditions: Seq[TransactionSingleCondition], updates: Seq[Record])

  case class TransactionGood(transaction: TransactionId, ids: Seq[RecordId]) extends TransactionResponse

  case class TransactionImpossible(found: Seq[TransactionSingleConditionResult]) extends TransactionResponse

  case class TransactionBad(throwable: Throwable) extends TransactionResponse

}
