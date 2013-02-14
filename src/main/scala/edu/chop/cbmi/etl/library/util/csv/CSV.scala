package edu.chop.cbmi.etl.library.util.csv

import scala.io.Source

import util.parsing.combinator.RegexParsers
import util.parsing.combinator.Parsers.Parser
import util.parsing.combinator.Parsers.Success
import edu.chop.cbmi.etl.library.util.CSV.CSVParser

/**
 * Created with IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/14/13
 * Time: 11:09 AM
 * To change this template use File | Settings | File Templates.
 */

class CSV(COMMA: String = ",") extends RegexParsers {

  override protected val whiteSpace = """[ \t]""".r

  def DQUOTE = "\""
  def DQUOTE2 = "\"\"" ^^ { case _ => "\"" }
  def CR = "\r"
  def LF = "\n"
  def CRLF = "\r\n"
  def TXT = "[^\"%s\r\n]".format(COMMA).r

  def file: Parser[List[List[String]]] = repsep(record, CRLF) <~ opt(CRLF)
  def record: Parser[List[String]] = rep1sep(field, COMMA)
  def field: Parser[String] = (escaped|nonescaped)
  def escaped: Parser[String] = (DQUOTE~>((TXT|COMMA|CR|LF|DQUOTE2)*)<~DQUOTE) ^^ { case ls => ls.mkString("")}
  def nonescaped: Parser[String] = (TXT*) ^^ { case ls => ls.mkString("") }

  def parse(i: scala.io.BufferedSource): List[Map[String, String]] = parse(i.getLines.mkString("\r\n"))
  def parse(i: scala.io.BufferedSource, headerLineNumber: Int): List[Map[String, String]] = parse(i.getLines.drop(headerLineNumber - 1).mkString("\r\n"))
  def parse(s: String): List[Map[String, String]] = parseAll(file, s) match {
    case Success(alllines, _) =>
      val head = alllines.head
      alllines.drop(1) map { line =>
        var theMap = Map[String, String]()
        head.zipWithIndex.map {	e =>
        val fieldName = e._1.replaceAll("\\s+", "")

          if (fieldName != "") theMap = theMap ++ Map(fieldName -> line(e._2))
        }

        theMap

      }
    case _ => List[Map[String, String]]()
  }

}