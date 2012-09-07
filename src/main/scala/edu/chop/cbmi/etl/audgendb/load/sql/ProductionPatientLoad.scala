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

import edu.chop.cbmi.etl.util.FileProperties
import edu.chop.cbmi.etl.util.SqlDialect

import edu.chop.cbmi.dataExpress.dataModels.DataRow
import edu.chop.cbmi.dataExpress.backends.SqlBackendFactory

import java.util.Calendar

import scala.util.Random

case class ProductionPatientLoad(override val sdbfp: String,
                                 override val tdbfp: String,
                                 override val sourceTableNames: Seq[String],
                                 override val targetTableNames: Seq[String],
                                 override val sourceDbSchemaName: Seq[Option[String]],
                                 override val targetDbSchemaName: Seq[Option[String]],
                                 override val query: Option[String])
  extends AudgendbSqlLoad(sdbfp, tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {

  val randomNumberGenerator = new Random()

  val dobOffSetRange = -15 to 15

  def append(): Boolean = {
    //Backend Connections are already made

    val targetSchemaName = targetDbSchemaName(0)

    val patientTableName = targetTableNames(0)

    val patientPhiTableName = targetTableNames(1)

    try {

      generateAppendPatientResultSet

      targetBackend.connection.setAutoCommit(false)

      while (sourceResult.next()) {


        sourceQuery =
          """
          INSERT INTO %s.%s (created,modified,alias,sex,ethnicity)                                                                                          +
          SELECT CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?,?,?
          """.format(targetSchemaName.get, patientTableName)


        var bindVars = DataRow(
          ("alias", sourceResult.getString("narrative_deid")),
          ("sex", sourceResult.getInt("patient_id")),
          ("ethnicity", sourceResult.getInt("encounter_id"))
        )


        try {

          targetBackend.execute(sourceQuery, bindVars)

        }
        catch {
          case e: org.postgresql.util.PSQLException => {
            println("Appending Patient Records Failed: " + "\n" + sourceQuery + "\n")
            log.write(sourceQuery)
            close
            throw e
          }
          case e: RuntimeException => {
            println("Appending Patient Records Failed: " + "\n" + sourceQuery + "\n")
            log.write(sourceQuery)
            close
            throw e
          }

        }


        val dobOffSetInt = randomNumberGenerator.nextInt(dobOffSetRange.length)

        val dobOffSet = dobOffSetInt - localAbs(dobOffSetRange.min)

        val realDob = sourceResult.getDate("real_dob")

        val dob: Calendar = Calendar.getInstance

        dob.setTime(realDob)

        dob.add(Calendar.DAY_OF_MONTH, dobOffSet)

        sourceQuery =
          """"
          INSERT INTO %s.%s
          (created,modified,mrn,first_name,middle_name,last_name,dob,zipcode,patient_id,real_dob,dob_offset)                                                                                         +
          SELECT  CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?,?,?,?,?,?,?,?,?,?
          """.format(targetSchemaName.get, patientPhiTableName)



        bindVars = DataRow(
          ("mrn", sourceResult.getString("mrn")),
          ("first_name", sourceResult.getString("first_name")),
          ("middle_name", sourceResult.getString("middle_name")),
          ("last_name", sourceResult.getString("last_name")),
          ("dob", dob),
          ("zipcode", sourceResult.getString("zipcode")),
          ("patient_id", sourceResult.getInt("patient_id")),
          ("real_dob", realDob),
          ("dob_offset", dobOffSet)
        )



        try {

          targetBackend.execute(sourceQuery, bindVars)

        }
        catch {
          case e: org.postgresql.util.PSQLException => {
            println("Appending Patient Phi Records Failed: " + "\n" + sourceQuery + "\n")
            log.write(sourceQuery)
            close
            throw e
          }
          case e: RuntimeException => {
            println("Appending Patient Phi Records Failed: " + "\n" + sourceQuery + "\n")
            log.write(sourceQuery)
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

  def generateAppendPatientResultSet: Boolean = {

    val sourceSchemaName = sourceDbSchemaName(0)

    val patientTableName = sourceTableNames(0)

    val patientPhiTableName = sourceTableNames(1)

    sourceQuery =
      """
      SELECT
      p.alias AS alias, p.sex AS sex , p.ethnicity AS ethnicity , pi.mrn AS mrn ,
      pi.first_name AS first_name, pi.middle_name AS middle_name, pi.last_name AS last_name ,
      pi.dob AS dob , pi.zipcode AS zipcode , pi.patient_id AS patient_id,
      pi.real_dob AS real_dob , pi.dob_offset AS dob_offset
      FROM %s.%s  p, %s.%s pi
      WHERE p.id = pi.patient_id
      """.format(sourceSchemaName.get, patientTableName, sourceSchemaName.get, patientPhiTableName)


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


  def localAbs(x: Int) = if (x >= 0) x else -x

}

object ProductionPatientLoad {

  var defaultSourceTableNames: Seq[String] = Seq("staging_patient", "staging_patientphi")

  var defaultTargetTableNames: Seq[String] = Seq("production_patient", "production_patientphi")

  var defaultSourceSchemaName = "public"

  var defaultTargetSchemaName = "public"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/load-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/load-target.properties"


  def apply(): ProductionPatientLoad = {

    try {

      new ProductionPatientLoad(
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


  def apply(query: String): ProductionPatientLoad = {

    try {

      new ProductionPatientLoad(
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

