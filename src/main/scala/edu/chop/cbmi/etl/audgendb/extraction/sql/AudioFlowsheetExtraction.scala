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


case class AudioFlowsheetExtraction(override val sdbfp: String,
                                    override val tdbfp: String,
                                    override val sourceTableName: Seq[String],
                                    override val targetTableName: Seq[String],
                                    override val sourceDbSchemaName: Seq[Option[String]],
                                    override val targetDbSchemaName: Seq[Option[String]],
                                    override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp, tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object AudioFlowsheetExtraction {

  val clarityAudgenDbPatientTableName = "audgendb_patient"

  val clarityAudgenDbEncounterTableName = "audgendb_encounter"

  val defaultTargetTableName = "audio_flowsheet"

  val defaultSourceTableName = "IP_FLWSHT_REC"

  val defaultSourceSchemaName = "RESEARCH"

  val defaultTargetSchemaName = "qe11b"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/extraction-target.properties"


  def buildQuery(): String = {

    val query: String =
      """
      SELECT
      "IP_FLWSHT_REC"."INPATIENT_DATA_ID" "inpatient_data_id", "PAT_ENC"."PAT_ENC_CSN_ID" "pat_enc_csn_id",
      "LINE" "line", "IP_FLWSHT_MEAS"."FLO_MEAS_ID" "flo_meas_id", "FLO_MEAS_NAME" "flo_meas_name",
      "FLO_DIS_NAME" "flo_dis_name", "MEAS_VALUE" "meas_value",
      TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
      CAST(NULL AS CHAR(150)) AS "md5"
      FROM
      "IP_FLWSHT_REC" , "IP_FLWSHT_MEAS" , "IP_FLO_GP_DATA" ,
      "PAT_ENC" , "audgendb_encounter"
      WHERE
      "IP_FLWSHT_MEAS"."FSD_ID" = "IP_FLWSHT_REC"."FSD_ID" AND
      "IP_FLO_GP_DATA"."FLO_MEAS_ID" = "IP_FLWSHT_MEAS"."FLO_MEAS_ID" AND
      "PAT_ENC"."INPATIENT_DATA_ID" = "IP_FLWSHT_REC"."INPATIENT_DATA_ID" AND
      "PAT_ENC"."PAT_ENC_CSN_ID" = "audgendb_encounter"."pat_enc_csn_id" AND
      (
        "FLO_MEAS_NAME" IN
          (
            'AUDIO HP AGE WHEN HEARING LOSS IDENTIFIED','AUDIO CI MANUFACTURER LT', 'AUDIO CI MANUFACTURER RT',
            'AUDIO CI MODEL LT','AUDIO CI MODEL RT','AUDIO CI SURGERY DATE L','AUDIO CI SURGERY DATE R',
            'AUDIO HP DATE HEARING INSTRUMENT FIRST FIT','AUDIO HP DATE OF NEW HEARING LOSS WAS IDENTIFIED',
            'AUDIO DATE OF LAST IFSP/IEP','AUDIO EDUCATIONAL/COMMUNICATION PHILOSOPHY',
            'AUDIO HP EVIDENCE OF PROGRESSIVE HEARING LOSS','AUDIO FM RECEIVER MODEL L','AUDIO FM RECEIVER MODEL R',
            'AUDIO FM REC. MANUFACTURER LT','AUDIO FM RECEIVER MANUFACTURER RT','AUDIO FM TRANSMITTER MANUFACTURER',
            'AUDIO FM TRANSMITTER MODEL','AUDIO HP HEARING LOSS IS BILATERAL OR UNILATERAL','AUDIO HA MANUFACTURER LT',
            'AUDIO MANUFACTURER RT3','AUDIO HA MODEL L','AUDIO HA MODEL R','AUDIO HEARING THERAPIST SESSIONS',
            'AUDIO HP POSSIBLE AUDITORY NEUROPATHY','AUDIO PRIME SPEECH PRO MODEL L','AUDIO PRIME SPEECH PRO MODEL R',
            'AUDIO SEC SPEECH PRO MODEL L','AUDIO SEC SPEECH PRO MODEL R','AUDIO HP TYPE OF HEARING LOSS',
            'AUDIO CI MANUFACTURER L','AUDIO CI MANUFACTURER R','AUDIO CI MODEL L','AUDIO CI MODEL R',
            'AUDIO FM TRANS MODEL','AUDIO FM TRANS MODEL L','AUDIO HA MANUFACTURER RT'
          )
          OR
          "FLO_MEAS_NAME" LIKE 'AUDIO SPEECH LANGUAGE PATH SERVICES%'
      )
      """

    query
  }


  def apply(): AudioFlowsheetExtraction = {

    try {

      new AudioFlowsheetExtraction(
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


  def apply(query: String): AudioFlowsheetExtraction = {

    try {

      new AudioFlowsheetExtraction(
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

