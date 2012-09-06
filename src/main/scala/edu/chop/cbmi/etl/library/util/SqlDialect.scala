package edu.chop.cbmi.etl.util

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/17/12
 * Time: 2:45 PM
 * To change this template use File | Settings | File Templates.
 */


class SqlDialect {

  def identifierQuote(dbFileProperties: FileProperties): String = {

  var quote: String = ""

    try {




        val db_type: String = dbFileProperties.props.getProperty("jdbcUri").split(":")(1)
        db_type match {

          case "postgresql" =>  quote     = "\""
          case "mysql"      =>  quote     = "`"
          case "oracle"     =>  quote     = "\""
          case "sqlserver"  =>  quote     = "\""
          case _ => throw new RuntimeException("Unsupported database type: " + db_type)
        }
      } catch {
        case _ => throw new RuntimeException("Required property 'jdbcUri' not in properties file")
    }

  quote
  }
}