package edu.chop.cbmi.etl.audgendb.extraction.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */

import edu.chop.cbmi.etl.extraction.sql.SqlExtraction


case class CTEncounterExtraction(override val sdbfp: String,
                                 override val tdbfp: String,
                                 override val sourceTableName: Seq[String],
                                 override val targetTableName: Seq[String],
                                 override val sourceDbSchemaName: Seq[Option[String]],
                                 override val targetDbSchemaName: Seq[Option[String]],
                                 override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp, tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object CTEncounterExtraction {

  val clarityAudgenDbPatientTableName = "audgendb_patient"

  val defaultTargetTableName = "encounter"

  val defaultSourceTableName = "ENCOUNTER"

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
      SELECT  DISTINCT "PAT_ENC"."PAT_ENC_CSN_ID" "pat_enc_csn_id",
      "PAT_ENC"."PAT_ID" "pat_id", TO_CHAR("PAT_ENC"."EFFECTIVE_DATE_DT",'YYYY-MM-DD') "encounter_date",
      "PAT_ENC"."AGE" "age", "PAT_ENC"."ENC_TYPE_TITLE" "enc_type_title", "CLARITY_DEP"."DEPARTMENT_NAME" "department",
      "CLARITY_DEP"."SPECIALTY" "specialty", TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
      CAST(NULL AS CHAR(150)) AS "md5"
      FROM
      "ORDER_PROC", "PAT_ENC", "ORDER_RES_COMP_CMT",
      "audgendb_patient", "CLARITY_DEP"
      WHERE
      "PAT_ENC"."PAT_ENC_CSN_ID" = "ORDER_PROC"."PAT_ENC_CSN_ID"        AND
      "ORDER_RES_COMP_CMT"."ORDER_ID" = "ORDER_PROC"."ORDER_PROC_ID"    AND
      UPPER("ORDER_RES_COMP_CMT"."RESULTS_COMP_CMT") LIKE '%TEMPORAL%'  AND
      "ORDER_PROC"."PROC_ID" IN
      (
        SELECT "PROC_ID" FROM "CLARITY_EAP"
        WHERE
        regexp_like("PROC_CODE", '((704(80|50|86|22))|(76(375|391)))(\.|$)')
      )                                                                       AND
      ("ENC_CLOSED_YN" = 'Y' OR "ENC_CLOSED_YN" IS NULL)                      AND
      "PAT_ENC"."PAT_ID" = "audgendb_patient"."pat_id"                        AND
      "PAT_ENC"."EFFECTIVE_DEPT_ID" = "CLARITY_DEP"."DEPARTMENT_ID"           AND
      "PAT_ENC"."CONTACT_DATE" < (SYSDATE - 30)
      """

    query
  }


  def apply(): CTEncounterExtraction = {

    try {

      new CTEncounterExtraction(
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


  def apply(query: String): CTEncounterExtraction = {

    try {

      new CTEncounterExtraction(
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

