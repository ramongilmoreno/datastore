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
  trait Condition
  case class SingleCondition (left: FieldOrValue, operator: Operation, right: FieldOrValue) extends Condition
  case class AndCondition (left: Condition, right: Condition) extends Condition
  case class OrCondition (left: Condition, right: Condition) extends Condition
  case class Query (fields: List[Field], table: String, condition: Option[Condition])

  case object SELECT
  case object FROM
  case object WHERE
  case object COMMA
  case object OPEN
  case object CLOSE
  case object AND
  case object OR

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
  def open: QueryParser.Parser[OPEN.type] = """\(""".r ^^ (_ => OPEN )
  def close: QueryParser.Parser[CLOSE.type] = """\)""".r ^^ (_ => CLOSE )
  def and: QueryParser.Parser[AND.type] = { "and".r ^^ (_ => AND) }
  def or: QueryParser.Parser[OR.type] = { "or".r ^^ (_ => OR ) }
  def fields: Parser[List[Field]] =
    (id ~ comma ~ fields ^^ { case value ~ _ ~ rest => Field(value) :: rest }) |
    (id ^^ (value => List(Field(value))))
  def fieldOrValue: Parser[FieldOrValue] =
    id ^^ ( id => Field(id) ) |
    value ^^ ( value => Value(value) )

  def equalOperation: QueryParser.Parser[Equal.type] = "=".r ^^ (_ => Equal )
  def notEqualOperation: QueryParser.Parser[NotEqual.type] = "<>".r ^^ (_ => NotEqual )
  def operation: Parser[Operation] = equalOperation | notEqualOperation
  def singleCondition: Parser[SingleCondition] =
    fieldOrValue ~ operation ~ fieldOrValue  ^^ { case left ~ op ~ right => SingleCondition(left, op, right) }

  def condition: Parser[Condition] =
    open ~ condition ~ close ~ (((and | or) ~ condition)?) ^^ {
      case _ ~ condition ~ _ ~ Some(op ~ rest) => op match {
        case AND => AndCondition(condition, rest)
        case OR => OrCondition(condition, rest)
      }
      case _ ~ condition ~ _ ~ None => condition
    } |
    singleCondition ~ (((and | or) ~ condition)?) ^^ {
      case single ~ Some(op ~ rest) => op match {
        case AND => AndCondition(single, rest)
        case OR => OrCondition(single, rest)
      }
      case single ~ None => single
    }

  def query: Parser[Query] =
    select ~ fields ~ from ~ id ~ (where ~ condition?) ^^ {
      case _ ~ fields ~ _ ~ table ~ None => Query(fields, table, None)
      case _ ~ fields ~ _ ~ table ~ Some(_ ~ condition) => Query(fields, table, Some(condition))
    }

  def parse(in: CharSequence): ParseResult[Query] = parse(query, in)
}
