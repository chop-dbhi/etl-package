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


case class RadiologyIntExtraction(  override val sdbfp: String,
                                    override val tdbfp: String,
                                    override val sourceTableName:  Seq[String],
                                    override val targetTableName:  Seq[String],
                                    override val sourceDbSchemaName:  Seq[Option[String]],
                                    override val targetDbSchemaName:  Seq[Option[String]],
                                    override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp,tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object RadiologyIntExtraction  {

  val clarityAudgenDbPatientTableName                 = "audgendb_patient"

  val clarityAudgenDbEncounterTableName               = "audgendb_encounter"

  val defaultTargetTableName                          = "radiology_int"

  val defaultSourceTableName                          = "ORDER_RES_COMP_CMT"

  val defaultSourceSchemaName                         = "RESEARCH"

  val defaultTargetSchemaName                         = "qe11b"

  val defaultSourcePropertiesFP                       =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP                       =
    "conf/connection-properties/extraction-target.properties"


  def buildQuery(): String = {

    val query:  String  =
    """
    SELECT
    "audgendb_encounter"."pat_enc_csn_id" "pat_enc_csn_id",
    "ORDER_RES_COMP_CMT"."ORDER_ID" "order_id", "ORDER_RES_COMP_CMT"."COMPONENT_ID" "component_id",
    "ORDER_RES_COMP_CMT"."LINE_COMMENT" "line", "ORDER_RES_COMP_CMT"."RESULTS_COMP_CMT" "note_text",
    TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
    CAST(NULL AS CHAR(150)) AS "md5"
    FROM
    "ORDER_PROC" , "ORDER_RES_COMP_CMT" , "audgendb_encounter"
    WHERE
    "audgendb_encounter"."pat_enc_csn_id" = "ORDER_PROC"."PAT_ENC_CSN_ID"   AND
    "ORDER_PROC"."ORDER_PROC_ID" = "ORDER_RES_COMP_CMT"."ORDER_ID"          AND
    (
      regexp_like("PROC_CODE" , '((704(80|50|86|22))|(76(375|391)))(\.|$)') OR
      regexp_like("PROC_CODE" , '705(40|42|51|52|53)(\.|$)')
    )
    ORDER BY "ORDER_ID" , "line"
    """

    query
  }


  def apply(): RadiologyIntExtraction = {

    try {

      new RadiologyIntExtraction (
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



  def apply(query: String): RadiologyIntExtraction = {

    try {

      new RadiologyIntExtraction   (
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

