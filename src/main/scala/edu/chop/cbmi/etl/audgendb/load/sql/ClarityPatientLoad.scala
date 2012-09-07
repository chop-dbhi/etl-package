package edu.chop.cbmi.etl.audgendb.load.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */

import edu.chop.cbmi.etl.load.sql.SqlLoad


case class ClarityPatientLoad(override val sdbfp: String,
                              override val tdbfp: String,
                              override val sourceTableNames: Seq[String],
                              override val targetTableNames: Seq[String],
                              override val sourceDbSchemaName: Seq[Option[String]],
                              override val targetDbSchemaName: Seq[Option[String]],
                              override val query: Option[String])
  extends AudgendbSqlLoad(sdbfp, tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {}

object ClarityPatientLoad {

  val defaultSourceTableName = "patient"

  val defaultTargetTableName = "audgendb_patient"

  val defaultTargetSchemaName = "RESEARCH"

  val defaultSourceSchemaName = "qe11b"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/load-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/load-target.properties"

  def apply(): ClarityPatientLoad = {

    try {

      new ClarityPatientLoad(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        Seq(defaultSourceTableName), Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        None
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }


  def apply(query: String): ClarityPatientLoad = {

    try {

      new ClarityPatientLoad(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        Seq(defaultSourceTableName), Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(query)
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }

}

