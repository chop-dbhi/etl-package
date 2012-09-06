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


case class ProcedureExtraction(   override val sdbfp: String,
                                  override val tdbfp: String,
                                  override val sourceTableName:  Seq[String],
                                  override val targetTableName:  Seq[String],
                                  override val sourceDbSchemaName:  Seq[Option[String]],
                                  override val targetDbSchemaName:  Seq[Option[String]],
                                  override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp,tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object ProcedureExtraction  {

  val clarityAudgenDbPatientTableName           = "audgendb_patient"

  val clarityAudgenDbEncounterTableName         = "audgendb_encounter"

  val defaultTargetTableName                    = "procedure"

  val defaultSourceTableName                    = "clarity_tdl_tran"

  val defaultSourceSchemaName                   = "RESEARCH"

  val defaultTargetSchemaName                   = "qe11b"

  val defaultSourcePropertiesFP                 =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP                 =
    "conf/connection-properties/extraction-target.properties"


  def buildQuery(): String = {

    val lastRefreshDate: String         = "(SYSDATE - (365 * 3))"

    val query: String =
      """
      SELECT tran.tdl_id                as  "tdl_id" ,
      tran.pat_enc_csn_id               as  "pat_enc_csn_id",
      tran.orig_service_date            as  "orig_service_date",
      tran.int_pat_id                   as  "pat_id",
      clarity_eap.proc_code             as  "proc_code",
      LOCALTIMESTAMP                    as  "extract_date",
      CAST(NULL AS CHAR(150))           as  "md5"
      FROM
      clarity_tdl_tran tran,
      audgendb_pat_id_list,
      clarity_eap,
      pat_enc
      WHERE
      audgendb_pat_id_list.pat_id = tran.int_pat_id       AND
      tran.proc_id (+) = clarity_eap.proc_id              AND
      tran.detail_type = 1                                AND
      tran.pat_enc_csn_id   = pat_enc.pat_enc_csn_id      AND
      lower(pat_enc.enc_closed_yn) = 'y'
      """


    query
  }


  def apply(): ProcedureExtraction = {

    try {

      new ProcedureExtraction (
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



  def apply(query: String): ProcedureExtraction = {

    try {

      new ProcedureExtraction   (
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

