package edu.chop.cbmi.etl.audgendb.extraction

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */

import edu.chop.cbmi.etl.extraction.sql.SqlExtraction
import edu.chop.cbmi.etl.util.FileProperties
import edu.chop.cbmi.etl.util.SqlDialect


case class FlowSheetExtraction(  override val sdbfp: String,
                                 override val tdbfp: String,
                                 override val sourceTableName:  Seq[String],
                                 override val targetTableName:  Seq[String],
                                 override val sourceDbSchemaName:  Seq[Option[String]],
                                 override val targetDbSchemaName:  Seq[Option[String]],
                                 override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp,tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object FlowSheetExtraction  {

  val clarityAudgenDbPatientTableName                   = "audgendb_patient"

  val clarityAudgenDbEncounterTableName                 = "audgendb_encounter"

  val defaultTargetTableName                            = "flowsheet"

  val defaultSourceTableName                            = "IP_FLWSHT_MEAS"

  val defaultSourceSchemaName                           = "RESEARCH"

  var defaultTargetSchemaName                           = "qe11b"

  val defaultSourcePropertiesFP                         =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP                         =
    "conf/connection-properties/extraction-target.properties"


  def buildQuery(): String = {


    val query:  String  =
    """
    SELECT
    DISTINCT
    "IFR"."INPATIENT_DATA_ID" "inpatient_data_id",
    "ENC"."PAT_ENC_CSN_ID" "pat_enc_csn_id",
    "IFM"."LINE" "line",
    "IFM"."FLO_MEAS_ID" "flo_meas_id",
    "IFD"."FLO_MEAS_NAME" "flo_meas_name", "IFD"."FLO_DIS_NAME" "flo_dis_name",
    "IFM"."MEAS_VALUE" "meas_value",
    TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
    CAST(NULL AS CHAR(150)) AS "md5"
    FROM
    "PAT_ENC" ENC, "IP_FLO_GP_DATA" IFD, "IP_FLWSHT_MEAS" IFM, "IP_FLWSHT_REC" IFR
    WHERE
    "IFD"."FLO_MEAS_ID" = "IFM"."FLO_MEAS_ID" AND "IFM"."FSD_ID" = "IFR"."FSD_ID"     AND
    "ENC"."INPATIENT_DATA_ID" = "IFR"."INPATIENT_DATA_ID"                             AND
    "ENC"."PAT_ENC_CSN_ID" IN
    (
      SELECT "pat_enc_csn_id" FROM "audgendb_encounter"
    )
    """


    query
  }


  def apply(): FlowSheetExtraction = {

    try {

      new FlowSheetExtraction (
        defaultSourcePropertiesFP,            defaultTargetPropertiesFP,
        Seq(defaultSourceTableName),          Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(buildQuery())
      )
    }
    catch {
      case e:RuntimeException     => throw new RuntimeException
    }

  }



  def apply(query: String): FlowSheetExtraction = {

    try {

      new FlowSheetExtraction   (
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

