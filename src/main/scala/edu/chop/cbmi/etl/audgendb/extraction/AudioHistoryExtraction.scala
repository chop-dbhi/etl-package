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


case class AudioHistoryExtraction(  override val sdbfp: String,
                                    override val tdbfp: String,
                                    override val sourceTableName:  Seq[String],
                                    override val targetTableName:  Seq[String],
                                    override val sourceDbSchemaName:  Seq[Option[String]],
                                    override val targetDbSchemaName:  Seq[Option[String]],
                                    override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp,tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) { }


object AudioHistoryExtraction  {

  val clarityAudgenDbPatientTableName                 = "audgendb_patient"

  val clarityAudgenDbEncounterTableName               = "audgendb_encounter"

  val defaultTargetTableName                          = "audio_history"

  val defaultSourceTableName                          = "AMBULATORY_NOTES"

  val defaultSourceSchemaName                         = "RESEARCH"

  val defaultTargetSchemaName                         = "qe11b"

  val defaultSourcePropertiesFP                       =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP                       =
    "conf/connection-properties/extraction-target.properties"


  def buildQuery(): String = {


    val query: String =
    """
    SELECT
    "AMBULATORY_NOTES"."PAT_ID" "pat_id", "NOTE_CSN_ID" "note_csn_id ",
    "AMBULATORY_NOTES"."NOTE_ID" "note_id",
    "AMBULATORY_NOTES"."PAT_ENC_CSN_ID" "pat_enc_csn_id",
    "AMBULATORY_NOTES"."LINE" "line", "AMBULATORY_NOTES"."NOTE_TEXT" "note_text",
    TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
    CAST(NULL AS CHAR(150)) AS "md5"
    FROM
    "AMBULATORY_NOTES" , "audgendb_patient" ,
    (
      SELECT
      DISTINCT  NOTE_ID
      FROM
      "AMBULATORY_NOTES"
      WHERE
      "NOTE_TEXT" LIKE '%AUDIOLOGIC CASE HISTORY%'
    ) AN
    WHERE
    "AMBULATORY_NOTES"."NOTE_ID" = "AN"."NOTE_ID" AND
    "audgendb_patient"."pat_id" = "AMBULATORY_NOTES"."PAT_ID"
    ORDER BY "PAT_ENC_CSN_ID" , "LINE"
    """

    query
  }


  def apply(): AudioHistoryExtraction   =   {

    try {

      new AudioHistoryExtraction  (
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



  def apply(query: String): AudioHistoryExtraction  =  {

    try {

      new AudioHistoryExtraction  (
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

