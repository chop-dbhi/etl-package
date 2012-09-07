package edu.chop.cbmi.etl.audgendb.extraction.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/****Needs Correction****/
/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */
/****                ****/

import edu.chop.cbmi.etl.extraction.sql.SqlExtraction


case class SurgicalProcedureExtraction(override val sdbfp: String,
                                       override val tdbfp: String,
                                       override val sourceTableName: Seq[String],
                                       override val targetTableName: Seq[String],
                                       override val sourceDbSchemaName: Seq[Option[String]],
                                       override val targetDbSchemaName: Seq[Option[String]],
                                       override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp, tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object SurgicalProcedureExtraction {

  val clarityAudgenDbPatientTableName = "audgendb_patient"

  val clarityAudgenDbEncounterTableName = "audgendb_encounter"

  val defaultTargetTableName = "surgical_procedure"

  val defaultSourceTableName = "HSP_ADMIT_PROC"

  val defaultSourceSchemaName = "RESEARCH"

  val defaultTargetSchemaName = "qe11b"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/extraction-target.properties"

  def buildQuery(): String = {

    val lastRefreshDate: String = "(SYSDATE - (365 * 3))"

    val query: String =
      """
      SELECT
      DISTINCT
      "audgendb_encounter"."pat_enc_csn_id" "pat_enc_csn_id",
      "HSP"."PAT_ID" "pat_id", "EAP"."PROC_ID" "proc_id", "EAP"."PROC_NAME" "proc_name",
      "EAP"."PROC_CODE" "proc_code", "EAP"."PROC_CAT" "proc_cat",
      "HAP"."ADMIT_PROC_TEXT" "proc_text",
      "ETS"."NAME" "proc_type",
      TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
      CAST(NULL AS CHAR(150)) AS "md5" FROM "PAT_ENC_HSP" HSP, "audgendb_encounter",
      "HSP_ADMIT_PROC" HAP ,
      "CLARITY_EAP" EAP , "ZC_EAP_TYPE_OF_SER" ETS
      WHERE
      "HSP"."PAT_ENC_CSN_ID" = "audgendb_encounter"."pat_enc_csn_id"                                AND
      "HAP"."PAT_ENC_CSN_ID" = "HSP"."PAT_ENC_CSN_ID"                                               AND
      "EAP"."PROC_ID" = "HAP"."PROC_ID" AND "ETS"."EAP_TYPE_OF_SER_C" = "EAP"."EAP_TYPE_OF_SER_C"   AND
      "ETS"."NAME" = 'Surgery'
      ORDER BY
      "HSP".PAT_ID,"audgendb_encounter"."pat_enc_csn_id"
      """

    query
  }


  def apply(): SurgicalProcedureExtraction = {

    try {

      new SurgicalProcedureExtraction(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        Seq(defaultSourceTableName), Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(buildQuery())
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }


  def apply(query: String): SurgicalProcedureExtraction = {

    try {

      new SurgicalProcedureExtraction(
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

