package edu.chop.cbmi.etl.extraction.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/13/12
 * Time: 12:52 PM
 * To change this template use File | Settings | File Templates.
 */


import edu.chop.cbmi.dataExpress.dataModels.DataTable
import edu.chop.cbmi.dataExpress.dataModels.DataType
import edu.chop.cbmi.dataExpress.dataWriters.DataWriter
import edu.chop.cbmi.dataExpress.dataWriters.sql.SqlTableWriter

import edu.chop.cbmi.dataExpress.backends.SqlBackendFactory

import edu.chop.cbmi.dataExpress.logging.Log

import edu.chop.cbmi.etl.util.FileProperties
import edu.chop.cbmi.etl.util.SqlDialect

import edu.chop.cbmi.etl.library.statistics.statsd.StatsdClient

abstract case class SqlExtraction(  sdbfp: String,        tdbfp: String,
                                    sourceTableName:      Seq[String],          targetTableName:      Seq[String],
                                    sourceDbSchemaName:   Seq[Option[String]],  targetDbSchemaName:   Seq[Option[String]],
                                    query: Option[String])  {
//  Extract from Clarity/origin and Load to Local Database/Resource
//  The extraction process shall produce New data (MD5 Filtered) that is ready to be transformed

  //i.e. Origin (clarity), Local(Raw), Staging, or Production
  protected val etlSourceType         = "Clarity"
  //i.e. Origin (clarity), Local(Raw), Staging
  protected val etlTargetType         = "Local"

  var statsdHost                                = "resrhtiuws03.research.chop.edu"

  var statsdPort                                = 8125

  val statsClient:StatsdClient                  = new StatsdClient(statsdHost,statsdPort)

  //Source Database
  val sdb                                       = new FileProperties(sdbfp)
  //Target Database
  val tdb                                       = new FileProperties(tdbfp)

  protected val sourceDefaultSchemaName         = sourceDbSchemaName(0)

  protected val targetDefaultSchemaName         = targetDbSchemaName(0)

  protected val sourceDefaultTableName          = sourceTableName(0)

  protected val targetDefaultTableName          = targetTableName(0)

  protected val sourceBackend                   = SqlBackendFactory(sdb.props)

  protected val targetBackend                   = SqlBackendFactory(tdb.props)

  var overWriteOption: String                   = "append"

  protected val sqlDialectUtility               = new SqlDialect()

  protected val sourceIdentifierQuote           = sqlDialectUtility.identifierQuote(sdb)
                                                    //use sourceBackend.sqlDialect.quoteIdentifier() instead
  protected val targetIdentifierQuote           = sqlDialectUtility.identifierQuote(tdb)

  val   log                                     = Log(false,true)

  var   sourceQuery: String                     = ""

  var   targetQuery: String                     = ""

  protected var sourceRowCountQuery             = ""

  protected var targetRowCountQuery             = ""
  //Source
  if      ( !query.isEmpty)                    {   sourceQuery   = query.get   }
  else if ( sourceDefaultSchemaName.isEmpty)   {   sourceQuery   = "SELECT * FROM %s".format(sourceDefaultTableName) }
  else    { sourceQuery   = "SELECT * FROM %s.%s".format(sourceDefaultSchemaName.get,sourceDefaultTableName)  }
  sourceRowCountQuery     = "SELECT count(1) as count FROM (%s)".format(sourceQuery)

  //Target
  if (targetDefaultSchemaName.isEmpty)  {

    targetQuery                 = "SELECT * FROM %s order by 1".format(targetDefaultTableName)

    targetRowCountQuery         = "SELECT count(1) as count FROM %s".format(targetDefaultTableName)

  }
  else  {

    targetQuery                 = "SELECT * FROM %s.%s order by 1".format(targetDefaultSchemaName.get,targetDefaultTableName)

    targetRowCountQuery         = "SELECT count(1) as count FROM %s.%s".format(targetDefaultSchemaName.get,targetDefaultTableName)

  }

  //i.e. Clarity
  private lazy val sourceData                               =     DataTable(sourceBackend,sourceQuery)

  // Target data i.e.  - Raw (Local) Postgres (ETL Source) Data
  private lazy val targetData                               =     DataTable(targetBackend,targetQuery)

  //DataWriter to load local data to staging Resource
  private val targetDataWriter:   DataWriter                =     DataWriter(targetBackend,targetDefaultSchemaName)
  //Need to keep backend connection open or:   Required Property 'jdbcUri' not present



  sourceBackend.connect()
  //
  targetBackend.connect()
  //
  targetBackend.connection.setAutoCommit(false)
  //Load Origin Data to Staging Resource

  //For optional visibility into individual queries in any process
  var processQuery: String                          = null
  //For optional visibility into individual result sets in any process

  //  Having issues with the not included DE databases with the
  //  Scala REPL/interpreter/Service Provider mechanism, so
  //  using the postgres target for tryConnection instead
  //  of source

  val processResult                                 = tryConnection("target")


  def extract(): Boolean = {
    try   {

      targetBackend.connection.setAutoCommit(true)
      targetDataWriter.insert_table(targetDefaultTableName,sourceData.dataTypes,sourceData,
        if (overWriteOption == "drop") SqlTableWriter.OVERWRITE_OPTION_DROP else  SqlTableWriter.OVERWRITE_OPTION_APPEND)

    }
    catch {
      case e:java.sql.SQLException  =>

            close

            println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

            throw e
    }

    true

  }

  def count(tableName:String  = targetDefaultTableName): Int  = {

    if (targetDefaultSchemaName.isEmpty)  {

      processQuery   = "SELECT count(1) as count FROM %s limit 1".format(tableName)

    }
    else  {

      processQuery   = "SELECT count(1) as count FROM %s.%s limit 1".format(targetDefaultSchemaName.get,tableName)

    }

    try {

      val result  = targetBackend.executeQuery(processQuery)

      result.next()

      result.getInt("count")


    }
    catch {

      case e:java.sql.SQLException  =>    {

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
      }

    }

  }


  //This needs to override in concrete classes to match the source dbms that will be used

  def tryConnection(connectionSource:String = "source"):  java.sql.ResultSet  = {
    /*  This is really added so that the sourceResult can be instantiated at object construction time         */
    /*  and then accessed throughout the different functions                                                  */

    /*  This may need to be overridden if the target DBMS type is changed from Oracle                         */

    if        (connectionSource  ==  "source")    {

      processQuery      =   "select * from dual"

      sourceBackend.executeQuery(processQuery)

    }
    else if   (connectionSource  ==  "target")    {

      processQuery      =   "select (1) from %s.%s limit 1".format(targetDbSchemaName(0).get,targetTableName(0))

      targetBackend.executeQuery(processQuery)

    }
    else  {

      println("tryConnection needs either source or target connection")

      throw new RuntimeException

    }


  }


  def close:  Boolean = {

    log.close

    sourceBackend.close

    targetBackend.close

    true
  }




}