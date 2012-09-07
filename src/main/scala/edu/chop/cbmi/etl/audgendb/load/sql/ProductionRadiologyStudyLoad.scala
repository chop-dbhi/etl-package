package edu.chop.cbmi.etl.audgendb.load.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_producedure@augendb_production_data from public.staging_producedure@augendb_staging */

import edu.chop.cbmi.etl.load.sql.SqlLoad

import edu.chop.cbmi.dataExpress.dataModels.DataRow


case class ProductionRadiologyStudyLoad(override val sdbfp: String,
                                        override val tdbfp: String,
                                        override val sourceTableNames: Seq[String],
                                        override val targetTableNames: Seq[String],
                                        override val sourceDbSchemaName: Seq[Option[String]],
                                        override val targetDbSchemaName: Seq[Option[String]],
                                        override val query: Option[String])
  extends AudgendbSqlLoad(sdbfp, tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {

  def updateImpression(): Boolean = {
    //Backend Connections are already made

    val targetSchema = targetDbSchemaName(0)

    val targetTable = targetTableNames(0)

    try {

      generateUpdateImpressionResultSet

      targetBackend.connection.setAutoCommit(false)

      while (sourceResult.next()) {


        val radiologyStudyUpdateQuery =
          "Update %s.%s  SET impression = ? WHERE encounter_id = ?".format(targetSchema.get, targetTable)


        val bindVars = DataRow(("impression", sourceResult.getString("narrative_deid")),
          ("encounter_id", sourceResult.getInt("encounter_id"))
        )


        try {

          targetBackend.execute(radiologyStudyUpdateQuery, bindVars)

          /*
          log.write(radiologyStudyUpdateQuery + "\n" + sourceResult.getString("narrative_deid") + "\n"  +
            "Encounter ID:  " + sourceResult.getInt("encounter_id")   +   "\n"     ")
          */


        }
        catch {
          case e: org.postgresql.util.PSQLException => {
            println("Update Radiology Study Update Impression Failed on update: " + "\n" + radiologyStudyUpdateQuery + "\n")
            log.write(radiologyStudyUpdateQuery)
            close
            throw e
          }
          case e: RuntimeException => {
            println("Update Radiology Study Update Impression Failed on update: " + "\n" + radiologyStudyUpdateQuery + "\n")
            log.write(radiologyStudyUpdateQuery)
            close
            throw e
          }

        }

        targetBackend.commit
      }

      close

    }
    catch {
      case e: RuntimeException => println("Load Failed:  Closing Backend Connections")

      close

      throw e
    }

    true

  }

  def generateUpdateImpressionResultSet: Boolean = {

    val cdField1List = "'encounter.pat_enc_csn_id','mriencounter.pat_enc_csn_id','ctencounter.pat_enc_csn_id','hospitalencounter.pat_enc_csn_id'"

    val cdTarget = "staging_encounter"

    val sourceSchema = sourceDbSchemaName(0)

    val refreshDbSchemaName = "refresh01"

    sourceQuery =

      """
      SELECT DISTINCT
      ci.narrative_deid AS narrative_deid, ci.order_proc_id, cd.target_id AS encounter_id ,
      e.patient_id AS patient_id
      FROM
      %s.%s ci, %s.%s cop, %s.%s cd, %s.%s e, %s.%s con
      WHERE
      ci.order_proc_id  = cop.ORDER_PROC_ID                   AND
      cop.ORDER_PROC_ID = con.ORDER_PROC_ID                   AND
      CAST(cop.PAT_ENC_CSN_ID AS VARCHAR(20)) = cd.id_1       AND
      cd.target_id  = e.id        AND
      cd.field_1    in (%s)       AND
      cd.target     = '%s'        AND
      ((date(e.visit_date)  = date(con.PROC_BGN_TIME))        OR
      (date(e.visit_date)   = date(con.PE_ENTRY_TIME))        OR
      (date(e.visit_date)   = date(con.PE_CONTACT_DATE))      OR
      (date(e.visit_date)   = date(con.ORDER_TIME) )          OR
      (date(e.visit_date)   = date(con.PE_HOSP_ADMSN_TIME)))  AND
      e.id in (select distinct(encounter_id) FROM %s.%s)
      """.format(sourceTableNames(0), refreshDbSchemaName, sourceTableNames(1), sourceSchema.get, sourceTableNames(2),
        sourceSchema.get, sourceTableNames(3), refreshDbSchemaName, sourceTableNames(4), cdField1List,
        cdTarget, refreshDbSchemaName, sourceTableNames(5)
      )



    try {

      sourceResult = sourceBackend.executeQuery(sourceQuery, DataRow.empty)

    }
    catch {
      case e: java.sql.SQLException =>

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")
        close
        throw e

    }

    true

  }

  def appendImpression(): Boolean = {
    //Backend Connections are already made

    val targetSchema = targetDbSchemaName(0)

    try {

      generateAppendImpressionResultSet

      targetBackend.connection.setAutoCommit(false)

      while (sourceResult.next()) {

        val radiologyStudyAppendImpressionQuery =
          """
          INSERT INTO
          %s.%s
          (created, modified, impression, patient_id, encounter_id, reviewed, requested, relevant, image_published, exclude)
          SELECT
          CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?,?,?,?,'f','f','f','f','f'
          """.format(targetSchema.get, targetTableNames(0))


        val bindVars = DataRow(//("created",sourceResult.getString("narrative_deid")),
          //("modified",sourceResult.getString("narrative_deid")),
          ("impression", sourceResult.getString("narrative_deid")),
          ("patient_id", sourceResult.getInt("patient_id")),
          ("encounter_id", sourceResult.getInt("encounter_id"))
        )


        try {

          targetBackend.execute(radiologyStudyAppendImpressionQuery, bindVars)

        }
        catch {
          case e: org.postgresql.util.PSQLException => {
            println("Update Radiology Study Update Impression Failed on update: " + "\n" + radiologyStudyAppendImpressionQuery + "\n")
            log.write(radiologyStudyAppendImpressionQuery)
            close
            throw e
          }
          case e: RuntimeException => {
            println("Update Radiology Study Update Impression Failed on update: " + "\n" + radiologyStudyAppendImpressionQuery + "\n")
            log.write(radiologyStudyAppendImpressionQuery)
            close
            throw e
          }

        }

        targetBackend.commit

      }

      close


    }
    catch {
      case e: RuntimeException => println("Load Failed:  Closing Backend Connections")

      close

      throw e
    }

    true
  }

  def generateAppendImpressionResultSet: Boolean = {

    val cdField1List =
      "'encounter.pat_enc_csn_id','mriencounter.pat_enc_csn_id','ctencounter.pat_enc_csn_id','hospitalencounter.pat_enc_csn_id'"

    val cdTarget = "staging_encounter"

    val sourceSchema = sourceDbSchemaName(0)

    val refreshDbSchemaName = "refresh01"

    sourceQuery =
      """
      SELECT DISTINCT
      ci.narrative_deid AS narrative_deid, cd.target_id AS encounter_id , e.patient_id AS patient_id
      FROM
      %s.%s ci, %s.%s cop, %s.%s cd, %s.%s e, %s.%s con
      WHERE
      ci.order_proc_id  = cop.ORDER_PROC_ID     AND
      cop.ORDER_PROC_ID = con.ORDER_PROC_ID     AND
      CAST(cop.PAT_ENC_CSN_ID AS VARCHAR(20)) = cd.id_1           AND
      cd.target_id  = e.id                                        AND
      cd.field_1 in (%s)                                          AND
      cd.target     = '%s'                                        AND
      ((date(e.visit_date)  = date(con.PROC_BGN_TIME))    OR
      (date(e.visit_date)   = date(con.PE_ENTRY_TIME))    OR
      (date(e.visit_date)   = date(con.PE_CONTACT_DATE))  OR
      (date(e.visit_date)   = date(con.ORDER_TIME))       OR
      (date(e.visit_date)   = date(con.PE_HOSP_ADMSN_TIME)))      AND
      e.id not in (select distinct(encounter_id) FROM %s.%s)
      """.format(sourceSchema.get, sourceTableNames(0), refreshDbSchemaName, sourceTableNames(1),
        sourceSchema.get, sourceTableNames(2), sourceSchema.get, sourceTableNames(3),
        refreshDbSchemaName, sourceTableNames(4), cdField1List, cdTarget, refreshDbSchemaName,
        sourceTableNames(5)
      )



    try {

      sourceResult = sourceBackend.executeQuery(sourceQuery, DataRow.empty)

    }
    catch {
      case e: java.sql.SQLException =>

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")
        close
        throw e
    }

    true

  }


}

object ProductionRadiologyStudyLoad {

  val defaultSourceTableNames: Seq[String] = Seq("clarity_imaging_impressions", "clarity_order_proc",
    "core_datasource", "staging_encounter",
    "clarity_imaging_order_narrative", "production_radiologystudy"
  )

  val defaultTargetTableNames: Seq[String] = Seq("production_radiologystudy")

  val defaultSourceSchemaName = "public"

  val defaultTargetSchemaName = "public"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/load-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/load-target.properties"


  def apply(): ProductionRadiologyStudyLoad = {

    try {

      new ProductionRadiologyStudyLoad(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        defaultSourceTableNames, defaultTargetTableNames,
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        None
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }


  def apply(query: String): ProductionRadiologyStudyLoad = {

    try {

      new ProductionRadiologyStudyLoad(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        defaultSourceTableNames, defaultTargetTableNames,
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(query)
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }


}

