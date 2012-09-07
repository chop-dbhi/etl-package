package edu.chop.cbmi.etl.audgendb.extraction.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */


case class EthnicityExtraction(override val sdbfp: String,
                               override val tdbfp: String,
                               override val sourceTableName: Seq[String],
                               override val targetTableName: Seq[String],
                               override val sourceDbSchemaName: Seq[Option[String]],
                               override val targetDbSchemaName: Seq[Option[String]],
                               override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp, tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {}


object EthnicityExtraction {

  val clarityAudgenDbPatientTableName = "audgendb_patient"

  val defaultTargetTableName = "ethnicity"

  val defaultSourceTableName = "ZC_ETHNIC_GROUP"

  val defaultSourceSchemaName = "RESEARCH"

  val defaultTargetSchemaName = "qe11b"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/extraction-target.properties"

  def buildQuery(): String = {

    val query: String =
      """
      "SELECT
      "ZG"."ETHNIC_GROUP_C" "ethnic_group_c", "ZG"."NAME" "name", "ZG"."TITLE" "title", "ZG"."ABBR" "abbr",
      "ZG"."INTERNAL_ID" "internal_id",TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
      CAST(NULL AS CHAR(150)) AS "md5"
      FROM
      "ZC_ETHNIC_GROUP" ZG "
      """

    query
  }


  def apply(): EthnicityExtraction = {

    try {

      new EthnicityExtraction(
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


  def apply(query: String): EthnicityExtraction = {

    try {

      new EthnicityExtraction(
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

