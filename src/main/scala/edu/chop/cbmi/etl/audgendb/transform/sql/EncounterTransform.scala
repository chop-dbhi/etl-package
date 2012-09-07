package edu.chop.cbmi.etl.audgendb.transform.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.staging_encounter@augendb_staging from public.encounter@<some_local_clarity_data_database */


import edu.chop.cbmi.dataExpress.dataModels.DataRow


import java.util.Calendar


case class EncounterTransform(override val sdbfp: String,
                              override val tdbfp: String,
                              override val sourceTableNames: Seq[String],
                              override val targetTableNames: Seq[String],
                              override val sourceDbSchemaName: Seq[Option[String]],
                              override val targetDbSchemaName: Seq[Option[String]],
                              override val query: Option[String])
  extends AudgendbSqlTransform(sdbfp, tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {

  processBackend.connect()

  targetBackend.connection.setAutoCommit(false)

  override def append: Boolean = {
    /* Transform each row(s) of data for the entire table(s)*/
    try {

      generateSourceResultSet()

      resourceId(resourceName) = retrieveResourceId(resourceName)

      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-DD")

      var visitDate = new java.util.Date()

      var targetEncounterRow: DataRow[Any] = DataRow.empty

      var etlReferenceRow: DataRow[Any] = DataRow.empty

      var patientId = 0

      var encounterId = 0

      val lastInsertedEncounterIdQuery: String =
        """SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id""".format(targetDbSchemaName(0).get, targetTableNames(0))

      while (sourceResult.next() && clear) {

        val patientId = retrievePatientIdByMrn(sourceResult.getString("pat_mrn_id"))

        if (!patientId.isEmpty) {

          visitDate = dateFormat.parse(sourceResult.getString("encounter_date"))

          val createdDate = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

          val modifiedDate = createdDate

          targetEncounterRow = DataRow(("created", createdDate),
            ("modified", modifiedDate),
            ("visit_date", visitDate),
            //("department",sourceResult.getString("department")),
            ("specialty", sourceResult.getString("specialty")),
            ("patient_id", patientId.get),
            ("age", sourceResult.getObject("age"))
          )

          //Insert into staging_encounter
          targetBackend.insertRow(targetTableNames(0), targetEncounterRow, targetDbSchemaName(0))

          try {
            val lastInsertedEncounterIdResult = targetBackend.executeQuery(lastInsertedEncounterIdQuery)
            lastInsertedEncounterIdResult.next()
            encounterId = lastInsertedEncounterIdResult.getInt("id")
          }
          catch {

            case e: RuntimeException => println("last inserted encounter id could not be retrieved")

            throw e
          }



          etlReferenceRow = DataRow(("resource_id", resourceId(resourceName)),
            ("field_1", "encounter.pat_enc_csn_id"),
            ("id_1", sourceResult.getString("pat_enc_csn_id")),
            ("target", "staging_encounter"),
            ("target_id", encounterId)

          )

          //Insert into staging data source
          targetBackend.insertRow(etlReferenceTableName, etlReferenceRow, etlReferenceDbSchemaName)

          //Only Commit if All Inserts are successful
          targetBackend.commit()

        }

      }
      close

      true
    }
    catch {
      case e: RuntimeException => println("Transform Failed:  Closing Backend Connections")

      close

      throw e
    }

  }

  override def generateSourceResultSet(): Boolean = {

    sourceQuery = """
                    SELECT   e.*,p.pat_mrn_id FROM %s.%s e, %s.%s p
                    WHERE
                    e.pat_id = p.pat_id
                    """.format(sourceDbSchemaName(0).get, sourceTableNames(0), sourceDbSchemaName(0).get, sourceTableNames(1))



    try {

      sourceResult = sourceBackend.executeQuery(sourceQuery, DataRow.empty)

    }
    catch {
      case e: java.sql.SQLException =>

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
    }

    true

  }


  override def close: Boolean = {

    log.close()
    sourceBackend.close()
    targetBackend.close()
    processBackend.close()

    true
  }

  override def clear: Boolean = {

    true

  }

}


object EncounterTransform {

  val defaultSourceTableNames: Seq[String] = Seq("encounter", "patient", "staging_patientphi")

  val defaultTargetTableNames: Seq[String] = Seq("staging_encounter")

  val defaultSourceSchemaName = "qe11b"

  val defaultTargetSchemaName = "qe11c"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/transform-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/transform-target.properties"


  def apply(): EncounterTransform = {

    try {

      new EncounterTransform(
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


  def apply(query: String): EncounterTransform = {

    try {

      new EncounterTransform(
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

