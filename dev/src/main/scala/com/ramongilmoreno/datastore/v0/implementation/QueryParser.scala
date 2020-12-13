package com.ramongilmoreno.datastore.v0.implementation

import scala.util.matching.Regex
import scala.util.parsing.combinator._

object QueryParser extends RegexParsers {

  trait FieldOrValue
  case class Field (id: String) extends FieldOrValue
  case class Value (value: String) extends FieldOrValue
  sealed trait Operation
  object Equal extends Operation { override def toString: String = "=" }
  object NotEqual extends Operation { override def toString: String = "<>" }
  case class Condition (left: FieldOrValue, operator: Operation, right: FieldOrValue)
  case class Query (fields: List[Field], table: String, conditions: List[Condition])

  case object SELECT
  case object FROM
  case object WHERE
  case object COMMA
  case object AND

  // Skip spaces
  // https://enear.github.io/2016/03/31/parser-combinators/
  override def skipWhitespace = true
  override val whiteSpace: Regex = "[ \t\r\n\f]+".r

  def id: Parser[String] = "[A-Za-z0-9%]+".r ^^ { identity }
  def value: Parser[String] = """"[A-Za-z0-9%]+"""".r ^^ { x => x.substring(1, x.length - 1) }
  def select: QueryParser.Parser[SELECT.type] = "select".r ^^ (_ => SELECT )
  def from: QueryParser.Parser[FROM.type] = "from".r ^^ (_ => FROM )
  def where: QueryParser.Parser[WHERE.type] = "where".r ^^ (_ => WHERE)
  def comma: QueryParser.Parser[COMMA.type] = ",".r ^^ (_ => COMMA )
  def and: QueryParser.Parser[AND.type] = {
    "and".r ^^ (_ => AND)
  }
  def fields: Parser[List[Field]] =
    (id ~ comma ~ fields ^^ { case value ~ _ ~ rest => Field(value) :: rest }) |
    (id ^^ (value => List(Field(value))))
  def fieldOrValue: Parser[FieldOrValue] =
    id ^^ ( id => Field(id) ) |
    value ^^ ( value => Value(value) )
  def equalOperation: QueryParser.Parser[Equal.type] = "=".r ^^ (_ => Equal )
  def notEqualOperation: QueryParser.Parser[NotEqual.type] = "<>".r ^^ (_ => NotEqual )
  def operation: Parser[Operation] = equalOperation | notEqualOperation
  def condition: Parser[Condition] =
    fieldOrValue ~ operation ~ fieldOrValue  ^^ { case left ~ op ~ right => Condition(left, op, right) }
  def listOfConditions: Parser[List[Condition]] =
    (condition ~ and ~ listOfConditions ^^ { case condition ~ _ ~ rest => condition :: rest }) |
    (condition ^^ (condition => List(condition)))
  def conditions: Parser[List[Condition]] =
    where ~ listOfConditions ^^ { case _ ~ list => list }
  def query: Parser[Query] =
    select ~ fields ~ from ~ id ~ (conditions?) ^^ {
      case _ ~ fields ~ _ ~ table ~ conditions => Query(fields, table, conditions.getOrElse(List[Condition]()))
    }

  def parse(in: CharSequence): ParseResult[Query] = parse(query, in)
}
