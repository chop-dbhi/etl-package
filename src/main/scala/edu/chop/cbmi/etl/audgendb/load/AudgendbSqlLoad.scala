package edu.chop.cbmi.etl.audgendb.load

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 7/13/12
 * Time: 11:09 AM
 * To change this template use File | Settings | File Templates.
 */

import  edu.chop.cbmi.etl.load.sql.SqlLoad

import edu.chop.cbmi.dataExpress.dataModels.DataTable
import edu.chop.cbmi.dataExpress.backends.SqlBackendFactory

import edu.chop.cbmi.etl.util.FileProperties

import scala.collection.mutable.MutableList
import scala.util.Random

case class AudgendbSqlLoad(override val sdbfp: String,
                           override val tdbfp: String,
                           override val sourceTableNames:   Seq[String],
                           override val targetTableNames:   Seq[String],
                           override val sourceDbSchemaName: Seq[Option[String]],
                           override val targetDbSchemaName: Seq[Option[String]],
                           override val query: Option[String])
  extends SqlLoad(sdbfp, tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {



}

