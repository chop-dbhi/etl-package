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
import edu.chop.cbmi.etl.util.FileProperties
import edu.chop.cbmi.etl.util.SqlDialect


case class PatientExtraction(override val sdbfp: String,
                             override val tdbfp: String,
                             override val sourceTableName: Seq[String],
                             override val targetTableName: Seq[String],
                             override val sourceDbSchemaName: Seq[Option[String]],
                             override val targetDbSchemaName: Seq[Option[String]],
                             override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp, tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object PatientExtraction {

  val defaultTargetTableName = "patient"

  val defaultSourceTableName = "PATIENT"

  val defaultSourceSchemaName = "RESEARCH"

  val defaultTargetSchemaName = "qe11b"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/extraction-target.properties"

  def buildQuery(): String = {

    val lastRefreshDate: String = "(SYSDATE - (365 * 30))"

    val query: String =
      """
      SELECT
      DISTINCT
      "PATIENT"."PAT_MRN_ID" "pat_mrn_id", "PATIENT"."PAT_ID" "pat_id", "PATIENT"."PAT_NAME" "pat_name",
      "PATIENT"."PAT_FIRST_NAME" "pat_first_name", "PATIENT"."PAT_MIDDLE_NAME" "pat_middle_name",
      "PATIENT"."PAT_LAST_NAME" "pat_last_name", TO_CHAR("PATIENT"."BIRTH_DATE",'YYYY-MM-DD') "birth_date",
      TO_CHAR("PATIENT"."DEATH_DATE",'YYYY-MM-DD') "death_date",  CAST("PATIENT"."SEX"  AS CHAR(4)) "sex",
      "PATIENT"."ZIP" "zip", "ZC_ETHNIC_GROUP"."NAME" "ethnic_group_name",
      TO_CHAR("PATIENT"."UPDATE_DATE",'YYYY-MM-DD') "cl_update_date",
      TO_CHAR("PATIENT"."REC_CREATE_DATE",'YYYY-MM-DD') "cl_create_date",
      TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
      CAST(NULL AS CHAR(150)) AS "md5"
      FROM
      "PATIENT", "ZC_ETHNIC_GROUP", "PAT_ENC"
      WHERE
      "PATIENT"."ETHNIC_GROUP_C" = "ZC_ETHNIC_GROUP"."INTERNAL_ID"(+)                                       AND
      "PATIENT"."PAT_ID" = "PAT_ENC"."PAT_ID" AND "PAT_ENC"."CONTACT_DATE" BETWEEN (SYSDATE - (365 * 30))   AND
      (sysdate - 30) AND
      "PATIENT"."PAT_NAME" NOT IN
      ('BUDDY,TEST X','TAYL,JUSTTWO','CADENCE,CHARLES B','CADENCE,DAVID B','TESTA,BABYBOY','SAMS,DEJA')     AND
      "PATIENT"."PAT_NAME" not like 'TEST MD%'  AND
      "PATIENT"."PAT_NAME" not like 'TEST V%'   AND
      "PATIENT"."PAT_NAME" not like 'ZZ%'       AND
      "PATIENT"."PAT_NAME" not like 'ZZZ%'      AND
      "PAT_ENC"."DEPARTMENT_ID" IN
      (89254028,87498028,80506028,84260028,81263028,92139043,83256028,80258028,82265028,101016026)  AND
      "PATIENT"."PAT_ID" IN
      (
        SELECT "PAT_ID" FROM "RESEARCH"."AUDGENDB_PAT_ID_LIST"
      )
      """

    query
  }


  def apply(): PatientExtraction = {

    try {

      new PatientExtraction(
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


  def apply(query: String): PatientExtraction = {

    try {

      new PatientExtraction(
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

