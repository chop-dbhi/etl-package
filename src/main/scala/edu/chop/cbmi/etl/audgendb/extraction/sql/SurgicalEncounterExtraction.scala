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


case class SurgicalEncounterExtraction(override val sdbfp: String,
                                       override val tdbfp: String,
                                       override val sourceTableName: Seq[String],
                                       override val targetTableName: Seq[String],
                                       override val sourceDbSchemaName: Seq[Option[String]],
                                       override val targetDbSchemaName: Seq[Option[String]],
                                       override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp, tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object SurgicalEncounterExtraction {

  val clarityAudgenDbPatientTableName = "audgendb_patient"

  val defaultSourceTableName = "ENCOUNTER"

  val defaultSourceSchemaName = "RESEARCH"

  val defaultTargetSchemaName = "qe11b"

  val defaultTargetTableName = "encounter"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/extraction-target.properties"

  def buildQuery(): String = {


    val query: String =
      """
      SELECT
      DISTINCT
      "HSP"."PAT_ENC_CSN_ID" "pat_enc_csn_id",
      "HSP"."PAT_ID" "pat_id",
      TO_CHAR("HSP"."CONTACT_DATE",'YYYY-MM-DD') "encounter_date",
      "PAT_ENC"."AGE" "age",
      "PAT_ENC"."ENC_TYPE_TITLE" "enc_type_title",
      "CLARITY_DEP"."DEPARTMENT_NAME" "department",
      "CLARITY_DEP"."SPECIALTY" "specialty",
      TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
      CAST(NULL AS CHAR(150)) AS "md5"
      FROM
      "PAT_ENC_HSP" HSP, "audgendb_patient" AGDB,
      "HSP_ADMIT_PROC" HAP,
      "CLARITY_EAP" EAP, "ZC_EAP_TYPE_OF_SER" ETS, "PAT_ENC", "CLARITY_DEP"
      WHERE
      "HSP"."PAT_ID" = "AGDB"."pat_id"                                                        AND
      "HAP"."PAT_ENC_CSN_ID" = "HSP"."PAT_ENC_CSN_ID" AND "EAP"."PROC_ID" = "HAP"."PROC_ID"   AND
      "ETS"."EAP_TYPE_OF_SER_C" = "EAP"."EAP_TYPE_OF_SER_C" AND "ETS"."NAME" = 'Surgery'      AND
      "PAT_ENC"."EFFECTIVE_DEPT_ID" = "CLARITY_DEP"."DEPARTMENT_ID"                           AND
      "PAT_ENC"."PAT_ENC_CSN_ID" = "HSP"."PAT_ENC_CSN_ID"                                     AND
      "PAT_ENC"."CONTACT_DATE" < (SYSDATE - 30)
      """

    query
  }


  def apply(): SurgicalEncounterExtraction = {

    try {

      new SurgicalEncounterExtraction(
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


  def apply(query: String): SurgicalEncounterExtraction = {

    try {

      new SurgicalEncounterExtraction(
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

