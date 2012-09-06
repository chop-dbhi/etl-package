package edu.chop.cbmi.etl.audgendb.load

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load clarity.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */

import edu.chop.cbmi.etl.load.sql.SqlLoad
import edu.chop.cbmi.etl.util.FileProperties


case class ClarityEncounterLoad(  override val sdbfp: String,
                                  override val tdbfp: String,
                                  override val sourceTableNames:    Seq[String],
                                  override val targetTableNames:    Seq[String],
                                  override val sourceDbSchemaName:  Seq[Option[String]],
                                  override val targetDbSchemaName:  Seq[Option[String]],
                                  override val query: Option[String])
  extends AudgendbSqlLoad(sdbfp,tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {}


object ClarityEncounterLoad  {

  var defaultTargetTableName                          = "audgendb_encounter"

  var defaultSourceTableName                          = "encounter"

  var defaultTargetSchemaName                         = "RESEARCH"

  var defaultSourceSchemaName                         = "qe11b"

  val defaultSourcePropertiesFP                       =
    "conf/connection-properties/load-source.properties"

  val defaultTargetPropertiesFP                       =
    "conf/connection-properties/load-target.properties"


  def apply(): ClarityEncounterLoad = {

    try {

      new ClarityEncounterLoad(
        defaultSourcePropertiesFP,            defaultTargetPropertiesFP,
        Seq(defaultSourceTableName),          Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        None
      )
    }
    catch {
      case e:RuntimeException     => throw new RuntimeException
    }

  }



  def apply(query: String): ClarityEncounterLoad = {

    try {

      new ClarityEncounterLoad(
        defaultSourcePropertiesFP,            defaultTargetPropertiesFP,
        Seq(defaultSourceTableName),          Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(query)
      )
    }
    catch {
      case e:RuntimeException     => throw new RuntimeException
    }

  }

}

