package edu.chop.cbmi.etl.transform.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/13/12
 * Time: 12:52 PM
 * To change this template use File | Settings | File Templates.
 */


import edu.chop.cbmi.etl.util.FileProperties
import edu.chop.cbmi.etl.util.SqlDialect

import edu.chop.cbmi.dataExpress.logging.Log

import edu.chop.cbmi.dataExpress.backends.SqlBackendFactory


import edu.chop.cbmi.etl.library.statistics.statsd.StatsdClient

abstract case class SqlTransform(   sdbfp: String,                                tdbfp: String,
                                    sourceTableNames:  Seq[String],               targetTableNames:  Seq[String],
                                    sourceDbSchemaName:  Seq[Option[String]],     targetDbSchemaName:  Seq[Option[String]],
                                    query: Option[String])  {

//i.e. Extract from Clarity/origin and Load to Local Database/Resource
  //i.e. Origin (clarity), Local(Raw), Staging, or Production
  protected val etlSourceType         = "\n"
  //i.e. Origin (clarity), Local(Raw), Staging
  protected val etlTargetType         = "\n"

  var statsdHost                            =     "resrhtiuws03.research.chop.edu"

  var statsdPort                            =     8125

  val statsClient:StatsdClient              =     new StatsdClient(statsdHost,statsdPort)

  val sdb                                   =     FileProperties(sdbfp)
  //Target Database
  val tdb                                   =     FileProperties(tdbfp)
  //Process Database
  var pdb                                   =     FileProperties(tdbfp)

  protected val sqlDialectUtility           =     new SqlDialect()

  val sourceBackend     =   SqlBackendFactory(sdb.props)  //Hard Coded For Now

  val targetBackend     =   SqlBackendFactory(tdb.props)  //Hard Coded For Now

  var processBackend    =   SqlBackendFactory(pdb.props)

  protected var sourceIdentifierQuote   = sqlDialectUtility.identifierQuote(sdb)

  protected var targetIdentifierQuote   = sqlDialectUtility.identifierQuote(tdb)
  
  val   log   =   Log(false,true)

  var   sourceQuery:  String                =   null

  var   processQuery: String                =   null

  sourceBackend.connect

  targetBackend.connect

  var sourceResult            = tryConnection

  def transform():  Boolean   = { false }
  //This needs to override in concrete classes to match the source dbms that will be used
  def tryConnection():  java.sql.ResultSet  = {
    /*  This is really added so that the sourceResult can be instantiated at object construction time       */
    /*  and then accessed throughout the different functions                                                */

    sourceQuery                                            =
      "SELECT (1) FROM %s.%s".format(sourceDbSchemaName(0).get,sourceTableNames(0))



    sourceBackend.executeQuery(sourceQuery)

  }

  def truncate():  Boolean =    {

    targetBackend.truncateTable(targetTableNames(0),targetDbSchemaName(0))

    targetBackend.truncateTable(targetTableNames(1),targetDbSchemaName(0))

    targetBackend.commit()

    true
  }

  def close(): Boolean  = {

    log.close
    sourceBackend.close
    targetBackend.close

    true

  }

  def clear():  Boolean       = { false }

}