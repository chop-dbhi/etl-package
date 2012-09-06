package edu.chop.cbmi.etl.load.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/13/12
 * Time: 12:52 PM
 * To change this template use File | Settings | File Templates.
 */


import edu.chop.cbmi.dataExpress.dataModels.DataTable
import edu.chop.cbmi.dataExpress.dataWriters.DataWriter
import edu.chop.cbmi.dataExpress.dataWriters.sql.SqlTableWriter
import edu.chop.cbmi.dataExpress.dataModels.DataRow

import edu.chop.cbmi.dataExpress.backends.SqlBackendFactory
import edu.chop.cbmi.etl.util.{SqlDialect, FileProperties}

import edu.chop.cbmi.dataExpress.logging.Log

import edu.chop.cbmi.etl.library.statistics.statsd.StatsdClient

case class SqlLoad( sdbfp: String,
                    tdbfp: String,
                    sourceTableNames:     Seq[String],
                    targetTableNames:     Seq[String],
                    sourceDbSchemaName:   Seq[Option[String]],
                    targetDbSchemaName:   Seq[Option[String]],
                    query: Option[String])  {


  //i.e. Extract from Clarity/origin and Load to Local Database/Resource
  //i.e. Origin (clarity), Local(Raw), Staging, or Production
  protected val etlSourceType         = "\n"
  //i.e. Origin (clarity), Local(Raw), Staging
  protected val etlTargetType         = "\n"

  var statsdHost                            =     "resrhtiuws03.research.chop.edu"

  var statsdPort                            =     8125

  val statsClient:StatsdClient              =     new StatsdClient(statsdHost,statsdPort)

  //Source Database
  val sdb                             = new FileProperties(sdbfp)
  //Target Database
  val tdb                             = new FileProperties(tdbfp)

  protected val sourceDefaultSchemaName         = sourceDbSchemaName(0)

  protected val targetDefaultSchemaName         = targetDbSchemaName(0)

  protected val sourceDefaultTableName          = sourceTableNames(0)

  protected val targetDefaultTableName          = targetTableNames(0)

  protected val sourceBackend = SqlBackendFactory(sdb.props)  //Hard Coded For Now

  protected val targetBackend = SqlBackendFactory(tdb.props)  //Hard Coded For Now

  var overWriteOption: String             = "append"

  val sqlDialectUtility                   = new SqlDialect

  protected var sourceIdentifierQuote     = sqlDialectUtility.identifierQuote( sdb)

  protected var targetIdentifierQuote     = sqlDialectUtility.identifierQuote( tdb)

  var log                                 = Log(false,true)

  var sourceQuery: String                 = ""

  var targetQuery: String                 = ""

  protected var sourceRowCountQuery       = ""

  protected var targetRowCountQuery       = ""


  //Source
  if      ( !query.isEmpty)                    {   sourceQuery   = query.get   }
  else if ( sourceDefaultSchemaName.isEmpty)   {   sourceQuery   = "SELECT * FROM %s".format(sourceDefaultTableName) }
  else    { sourceQuery   = "SELECT * FROM %s.%s".format(sourceDefaultSchemaName.get,sourceDefaultTableName)  }
  sourceRowCountQuery     = "SELECT count(1) as count FROM (%s)".format(sourceQuery)

  //Target
  if (targetDefaultSchemaName.isEmpty)  {

    targetQuery                 = "SELECT * FROM %s".format(targetDefaultTableName)

    targetRowCountQuery         = "SELECT count(1) as count FROM %s".format(targetDefaultTableName)

  }
  else  {

    targetQuery                 = "SELECT * FROM %s.%s".format(targetDefaultSchemaName.get,targetDefaultTableName)

    targetRowCountQuery         = "SELECT count(1) as count FROM %s.%s".format(targetDefaultSchemaName.get,targetDefaultTableName)

  }


  //Local Resource/Table with the written data
  private val sourceData                      =   DataTable(sourceBackend,sourceQuery)
  //DataWriter to load local data to staging Resource
  private val targetDataWriter:   DataWriter  =   DataWriter(targetBackend,targetDbSchemaName(0))
  //Need to keep backend connection open or:   Required Property 'jdbcUri' not present
  //
  sourceBackend.connect()
  //
  targetBackend.connect()
  //
  targetBackend.connection.setAutoCommit(false)

  //Load Origin Data to Staging Resource
  //For optional visibility into individual queries in any process
  var processQuery: String                          = null
  //For optional visibility into individual result sets in any process
  var sourceResult                                  = tryConnection



  def load: Boolean = {
    try   {

      targetBackend.connection.setAutoCommit(true)
      targetDataWriter.insert_table(targetTableNames(0),sourceData.dataTypes,sourceData,
        if (overWriteOption == "drop") SqlTableWriter.OVERWRITE_OPTION_DROP else  SqlTableWriter.OVERWRITE_OPTION_APPEND)

      true
    }
    catch {
      case e:java.sql.SQLException  =>

            close

            println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

            throw e
    }
  }

  def countMatch():  Boolean  =   {
    try {

      val sourceRowCountResult  =   sourceBackend.executeQuery(sourceRowCountQuery,DataRow.empty)

      val targetRowCountResult  =   targetBackend.executeQuery(targetRowCountQuery,DataRow.empty)

      sourceRowCountResult.next()

      targetRowCountResult.next()

      (sourceRowCountResult.getInt("count") == targetRowCountResult.getInt("count") )
    }
    catch {
      case e:java.sql.SQLException  =>

            close

            println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

            throw e
    }
  }

  def tryConnection:  java.sql.ResultSet  = {
    /*  This is really added so that the sourceResult can be instantiated at object construction time       */
    /*  and then accessed throughout the different functions                                                */

    /*  This may need to be overridden if the target DBMS type is changed from Postgres                     */

    processQuery  =
    if ( sourceDefaultSchemaName.isEmpty)   {   "SELECT * FROM %s limit 1".format(sourceDefaultTableName) }
    else    {   "SELECT * FROM %s.%s limit 1".format(sourceDefaultSchemaName.get,sourceDefaultTableName)  }

    sourceBackend.executeQuery(processQuery)

  }

  def close():Boolean   =   {

    log.close

    sourceBackend.close

    targetBackend.close

    true

  }



}
