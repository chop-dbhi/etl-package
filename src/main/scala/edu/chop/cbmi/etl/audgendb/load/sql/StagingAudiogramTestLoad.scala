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
import edu.chop.cbmi.etl.util.FileProperties


case class StagingAudiogramTestLoad(override val sdbfp: String,
                                    override val tdbfp: String,
                                    override val sourceTableNames: Seq[String],
                                    override val targetTableNames: Seq[String],
                                    override val sourceDbSchemaName: Seq[Option[String]],
                                    override val targetDbSchemaName: Seq[Option[String]],
                                    override val query: Option[String])
  extends AudgendbSqlLoad(sdbfp, tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {}


object StagingAudiogramTestLoad {


  val defaultSourceTableNames = Seq("staging_audiogramresult")

  val defaultTargetTableNames = Seq("staging_audiogramresult")

  val defaultSourceSchemaName = "public"

  val defaultTargetSchemaName = "public"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/load-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/load-target.properties"


  def apply(): StagingAudiogramTestLoad = {

    try {

      new StagingAudiogramTestLoad(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        defaultSourceTableNames, defaultTargetTableNames,
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        None
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }


  def apply(query: String): StagingAudiogramTestLoad = {

    try {

      new StagingAudiogramTestLoad(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        defaultSourceTableNames, defaultTargetTableNames,
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(query)
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }

}

