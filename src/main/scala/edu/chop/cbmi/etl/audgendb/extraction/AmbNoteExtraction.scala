package edu.chop.cbmi.etl.audgendb.extraction

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */


case class AmbNoteExtraction(  override val sdbfp: String,
                               override val tdbfp: String,
                               override val sourceTableName:  Seq[String],
                               override val targetTableName:  Seq[String],
                               override val sourceDbSchemaName:  Seq[Option[String]],
                               override val targetDbSchemaName:  Seq[Option[String]],
                               override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp,tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) { }


object AmbNoteExtraction  {

  val clarityAudgenDbPatientTableName                 = "audgendb_patient"

  val clarityAudgenDbEncounterTableName               = "audgendb_encounter"

  val defaultTargetTableName                          = "amb_note"

  val defaultSourceTableName                          = "AMBULATORY_NOTES"

  val defaultSourceSchemaName                         = "RESEARCH"

  val defaultTargetSchemaName                         = "qe11b"

  val defaultSourcePropertiesFP                       =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP                       =
    "conf/connection-properties/extraction-target.properties"


  def buildQuery(): String = {

    val query: String =
      """"
      SELECT
      "AN"."PAT_ID" "pat_id", "AN"."NOTE_CSN_ID" "note_csn_id", "AN"."NOTE_ID" "note_id",
      "PAT_ENC_CSN_ID" "pat_enc_csn_id", "LINE" "line", "NOTE_TEXT" "note_text",
      TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
      CAST(NULL AS CHAR(150)) AS "md5"
      FROM
      "AMBULATORY_NOTES" AN,
      (
        SELECT
        DISTINCT "N"."ENCOUNTER_NOTE_ID",  MAX("N"."PAT_ENC_DATE_REAL") CONTACT_DATE_REAL ,
        CAST(NULL AS CHAR(70)) AS "MD5"
        FROM
        "ENC_NOTE_INFO" N  , "audgendb_encounter" APE
        WHERE
        "N"."PAT_ENC_CSN_ID" = "APE"."pat_enc_csn_id"
        GROUP BY "N"."ENCOUNTER_NOTE_ID"
      ) PTR
      WHERE
      "AN"."NOTE_ID" = "PTR"."ENCOUNTER_NOTE_ID" AND
      "AN"."CONTACT_DATE_REAL" = "PTR"."CONTACT_DATE_REAL"
      ORDER BY "AN"."NOTE_CSN_ID" , "AN"."LINE"
      """

    query
  }


  def apply(): AmbNoteExtraction = {

    try {

      new AmbNoteExtraction(
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

  

  def apply(query: String): AmbNoteExtraction = {

    try {

      new AmbNoteExtraction(
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

