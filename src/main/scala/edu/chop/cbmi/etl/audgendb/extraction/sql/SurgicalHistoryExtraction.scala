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


case class SurgicalHistoryExtraction(override val sdbfp: String,
                                     override val tdbfp: String,
                                     override val sourceTableName: Seq[String],
                                     override val targetTableName: Seq[String],
                                     override val sourceDbSchemaName: Seq[Option[String]],
                                     override val targetDbSchemaName: Seq[Option[String]],
                                     override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp, tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object SurgicalHistoryExtraction {

  val clarityAudgenDbPatientTableName = "audgendb_patient"

  val clarityAudgenDbEncounterTableName = "audgendb_encounter"

  val defaultTargetTableName = "surgical_history"

  val defaultSourceTableName = "surgical_hx "

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
      SELECT sh.pat_id      as  "pat_id",
      sh.line               as  "line",
      sh.proc_id            as  "proc_id",
      sh.surgical_hx_date   as  "surgical_hx_date",
      sh.comments           as  "comments",
      sh.proc_comments      as  "proc_comments",
      regexp_replace(sh.proc_code,'\.[0-9]+','' )   as  "cpt_code",
      sh.proc_code                                  as  "proc_code",
      sh.contact_date                               as  "contact_date",
      LOCALTIMESTAMP                                as  "extract_date"
      CAST(NULL AS CHAR(150))                       as  "md5"
      FROM
      surgical_hx sh,
      audgendb_pat_id_list a
      WHERE
      a.pat_id = sh.pat_id
      """

    query
  }


  def apply(): SurgicalHistoryExtraction = {

    try {

      new SurgicalHistoryExtraction(
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


  def apply(query: String): SurgicalHistoryExtraction = {

    try {

      new SurgicalHistoryExtraction(
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

