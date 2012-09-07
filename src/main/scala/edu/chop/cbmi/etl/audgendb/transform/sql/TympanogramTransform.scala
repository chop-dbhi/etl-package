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

import scala.collection.mutable.MutableList
import scala.collection.mutable.Map

import scala.util.matching.Regex

import scala.math.BigDecimal

import java.util.Calendar


case class TympanogramTransform(override val sdbfp: String, override val tdbfp: String,
                                override val sourceTableNames: Seq[String], override val targetTableNames: Seq[String],
                                override val sourceDbSchemaName: Seq[Option[String]], override val targetDbSchemaName: Seq[Option[String]],
                                override val query: Option[String])
  extends AudgendbSqlTransform(sdbfp, tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {

  val conceptIdsMap: Map[String, String] =

    Map(
      "frequencyMConceptId" -> "CHOP#AUDIO#0028",
      "rightMepConceptId" -> "CHOP#AUDIO#0042",
      "rightScConceptId" -> "CHOP#AUDIO#0052",
      "rightVolConceptId" -> "CHOP#AUDIO#0032",
      "rightNoteConceptId" -> "CHOP#AUDIO#0025",
      "rightTraceConceptId" -> "CHOP#AUDIO#0022",
      "leftMepConceptId" -> "CHOP#AUDIO#0043",
      "leftScConceptId" -> "CHOP#AUDIO#0053",
      "leftVolConceptId" -> "CHOP#AUDIO#0033",
      "leftNoteConceptId" -> "CHOP#AUDIO#0026",
      "leftTraceConceptId" -> "CHOP#AUDIO#0023"
    )


  val leftTympanogramResultMap: Map[String, Any] =

    Map(

      "frequency" -> null,
      "mep" -> null,
      "sc" -> null,
      "vol" -> null,
      "note" -> "",
      "trace" -> "",
      "comment" -> MutableList.empty
    )

  val rightTympanogramResultMap: Map[String, Any] =

    Map(
      "frequency" -> null,
      "mep" -> null,
      "sc" -> null,
      "vol" -> null,
      "note" -> "",
      "trace" -> "",
      "comment" -> MutableList.empty
    )


  val decimalRegexPattern = """[-+]?\s*(?:\d{0,3}\.\d+|\d{1,3}\.\d*|\d{1,3})"""

  val frequencyRegexPattern = """(\d+)\s*\w*"""

  val decimalRegex = new Regex(decimalRegexPattern)

  val frequencyRegex = new Regex(frequencyRegexPattern, "frequency")

  val encountersList: MutableList[String] = MutableList.empty

  val pressureRange = -395 to 200 by 5

  processBackend.connect()

  targetBackend.connection.setAutoCommit(false)

  override def append: Boolean = {
    /* Transform each row(s) of data for the entire table(s)*/
    try {

      generateSourceEncountersList

      resourceId(resourceName) = retrieveResourceId(resourceName)

      encountersList.foreach(pat_enc_csn_id => {

        val encounterConceptsResultSet = generateSourceConceptsResultSet(pat_enc_csn_id)

        val ids = retrieveEncounterIdPatientIdByPECId(pat_enc_csn_id)

        val encounterId = ids(0)

        val patientId = ids(1)

        while (encounterConceptsResultSet.next) {

          val conceptId = encounterConceptsResultSet.getString("concept_id")

          val conceptValue = encounterConceptsResultSet.getString("concept_value")


          if (conceptId == conceptIdsMap("frequencyMConceptId")) {

            val frequencyMList = frequencyRegex.findAllIn(conceptValue).matchData.toList

            if (frequencyMList.indices.length > 0) {

              leftTympanogramResultMap("frequency") = frequencyMList(0).group("frequency")
              rightTympanogramResultMap("frequency") = leftTympanogramResultMap("frequency")

            }
            else {

              leftTympanogramResultMap("frequency") = null
              rightTympanogramResultMap("frequency") = null

            }

          }


          /*    Left Side   */

          if (conceptId == conceptIdsMap("leftMepConceptId")) {

            val decimalMatchList = decimalRegex.findAllIn(conceptValue.replace("  ", " ")).matchData.toList

            if (decimalMatchList.indices.length > 0) {
              //group(0) will be the entire match
              leftTympanogramResultMap("mep") =
                BigDecimal(decimalMatchList(0).group(0).replace("  ", " "))

            }
            else {

              leftTympanogramResultMap("mep") = null

              leftTympanogramResultMap("comment") += conceptValue.replace("  ", " ")
            }

          }


          if (conceptId == conceptIdsMap("leftScConceptId")) {

            val decimalMatchList = decimalRegex.findAllIn(conceptValue.replace("  ", " ")).matchData.toList

            if (decimalMatchList.indices.length > 0) {
              //group(0) will be the entire match
              leftTympanogramResultMap("sc") =
                BigDecimal(decimalMatchList(0).group(0).replace("  ", " "))

            }
            else {

              leftTympanogramResultMap("sc") = null

              leftTympanogramResultMap("comment") += conceptValue.replace("  ", " ")
            }

          }


          if (conceptId == conceptIdsMap("leftVolConceptId")) {

            val decimalMatchList = decimalRegex.findAllIn(conceptValue.replace("  ", " ")).matchData.toList

            if (decimalMatchList.indices.length > 0) {
              //group(0) will be the entire match
              leftTympanogramResultMap("vol") =
                BigDecimal(decimalMatchList(0).group(0).replace("  ", " "))

            }
            else {

              leftTympanogramResultMap("vol") = null

              leftTympanogramResultMap("comment") += conceptValue.replace("  ", " ")
            }

          }
          if (conceptId == conceptIdsMap("leftNoteConceptId")) {


            if ((conceptValue != null) && (conceptValue != "")) {

              leftTympanogramResultMap("note") = conceptValue.trim()

            }
            else {

              leftTympanogramResultMap("note") = " "

            }


          }

          if (conceptId == conceptIdsMap("leftTraceConceptId")) {

            if ((conceptValue != null) && (conceptValue != "")) {

              leftTympanogramResultMap("trace") = parseTrace(conceptValue)

            }
            else {

              leftTympanogramResultMap("trace") = " "

            }

          }

          /*  End Left Side   */

          /*  Right Side      */
          if (conceptId == conceptIdsMap("rightMepConceptId")) {

            val decimalMatchList = decimalRegex.findAllIn(conceptValue.replace("  ", " ")).matchData.toList

            if (decimalMatchList.indices.length > 0) {
              //group(0) will be the entire match
              rightTympanogramResultMap("mep") =
                BigDecimal(decimalMatchList(0).group(0).replace("  ", " "))

            }
            else {

              rightTympanogramResultMap("mep") = null

              rightTympanogramResultMap("comment") += conceptValue.replace("  ", " ")
            }

          }
          if (conceptId == conceptIdsMap("rightScConceptId")) {

            val decimalMatchList = decimalRegex.findAllIn(conceptValue.replace("  ", " ")).matchData.toList

            if (decimalMatchList.indices.length > 0) {
              //group(0) will be the entire match
              rightTympanogramResultMap("sc") =
                BigDecimal(decimalMatchList(0).group(0).replace("  ", " "))

            }
            else {

              rightTympanogramResultMap("sc") = null

              rightTympanogramResultMap("comment") += conceptValue.replace("  ", " ")
            }

          }
          if (conceptId == conceptIdsMap("rightVolConceptId")) {

            val decimalMatchList = decimalRegex.findAllIn(conceptValue.replace("  ", " ")).matchData.toList
            //group(0) will be the entire match
            if (decimalMatchList.indices.length > 0) {

              rightTympanogramResultMap("vol") =
                BigDecimal(decimalMatchList(0).group(0).replace("  ", " "))

            }
            else {

              rightTympanogramResultMap("vol") = null

              rightTympanogramResultMap("comment") += conceptValue.replace("  ", " ")
            }

          }
          if (conceptId == conceptIdsMap("rightNoteConceptId")) {


            if ((conceptValue != null) && (conceptValue != "")) {

              rightTympanogramResultMap("note") = conceptValue.trim()

            }
            else {

              rightTympanogramResultMap("note") = " "

            }


          }

          if (conceptId == conceptIdsMap("rightTraceConceptId")) {

            if ((conceptValue != null) && (conceptValue != "")) {

              rightTympanogramResultMap("trace") = parseTrace(conceptValue)

            }
            else {

              rightTympanogramResultMap("trace") = " "

            }

          }

          /*    End Right Side    */

          else {
            /* doNothing */
          }


        } //  End While encounterConceptsResultSet.next


        /**Set Tympanogram Comments          **/
        if (
          (leftTympanogramResultMap("vol") != null) &&
            (leftTympanogramResultMap("sc") != null) &&
            (leftTympanogramResultMap("mep") != null)
        ) {

          leftTympanogramResultMap("comment") = "no peak"

        }
        else {

          Set(leftTympanogramResultMap("comment")).mkString("", ", ", "")

        }


        if (
          (rightTympanogramResultMap("vol") != null) &&
            (rightTympanogramResultMap("sc") != null) &&
            (rightTympanogramResultMap("mep") != null)
        ) {

          rightTympanogramResultMap("comment") = "no peak"

        }
        else {

          Set(rightTympanogramResultMap("comment")).mkString("", ", ", "")

        }

        /**End - Set Tympanogram Comments    **/


        /**Save to Database                  **/

        if (
          (leftTympanogramResultMap("trace") != null) ||
            (leftTympanogramResultMap("vol") != null) ||
            (leftTympanogramResultMap("mep") != null) ||
            (leftTympanogramResultMap("sc") != null) ||
            (leftTympanogramResultMap("note") != null)
        ) {

          saveLeftTympanogram(pat_enc_csn_id, patientId, encounterId)
          resetLeftTympanogramResultMap

        }


        if (
          (rightTympanogramResultMap("trace") != null) ||
            (rightTympanogramResultMap("vol") != null) ||
            (rightTympanogramResultMap("mep") != null) ||
            (rightTympanogramResultMap("sc") != null) ||
            (rightTympanogramResultMap("note") != null)
        ) {

          saveRightTympanogram(pat_enc_csn_id, patientId, encounterId)
          resetRightTympanogramResultMap
        }

        /**End - Save to Database            **/


        //Only Commit if All Inserts (Left and Right) are successful
        targetBackend.commit()

      }

      ) //  End - Foreach Encounter


      processBackend.close()

      sourceBackend.close()
      targetBackend.close()

      true

    }
    catch {
      case e: RuntimeException => println("Transform Failed:  Closing Backend Connections")

      processBackend.close()

      sourceBackend.close()
      targetBackend.close()

      throw e
    }

  }


  def saveLeftTympanogram(pat_enc_csn_id: String, patientId: Int, encounterId: Int): Unit = {

    /*  Store Date in Database      */
    /*  Insert for Tympanogram Test */
    /*  Insert one Tympanogram test record for each audiogramResultData */


    var tympanogramTestId = 0

    var tympanogramResultId = 0

    val createdDate = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

    val modifiedDate = createdDate

    val lastInsertedTympanogramTestIdQuery: String =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get, targetTableNames(0))


    val lastInsertedTympanogramResultIdQuery: String =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get, targetTableNames(1))


    /****  ** ** **    ***/
    //  Left              //
    /****  ** ** **    ***/

    /**Test        **/
    val targetTympanogramTestRow = DataRow(("created", createdDate),
      ("modified", modifiedDate),
      ("patient_id", patientId),
      ("encounter_id", encounterId)
    )

    //Insert into TympanogramTest
    targetBackend.insertRow(targetTableNames(0), targetTympanogramTestRow, targetDbSchemaName(0))

    try {
      val lastInsertedTympanogramTestIdResult = targetBackend.executeQuery(lastInsertedTympanogramTestIdQuery)
      lastInsertedTympanogramTestIdResult.next()
      tympanogramTestId = lastInsertedTympanogramTestIdResult.getInt("id")
    }
    catch {
      case e: RuntimeException => println("last inserted tympanogram test id could not be retrieved")
      throw e
    }

    var etlReferenceRow = DataRow(("resource_id", resourceId(resourceName)),
      ("field_1", "concept.pat_enc_csn_id"),
      ("id_1", pat_enc_csn_id),
      ("target", "staging_tympanogramtest"),
      ("target_id", tympanogramTestId)

    )

    //Insert into staging data source
    targetBackend.insertRow(etlReferenceTableName, etlReferenceRow, etlReferenceDbSchemaName)


    /**Result      **/

    val targetTympanogramResultRow =

      DataRow(("created", createdDate),
        ("modified", modifiedDate),
        ("ear", "Left"),
        ("probe_freq", (if (leftTympanogramResultMap.getOrElse("frequency", null) != null) {
          leftTympanogramResultMap("frequency").toString.toInt
        }
        else {
          null
        })
          ),
        ("mep", (if (leftTympanogramResultMap.getOrElse("mep", null) != null) {
          leftTympanogramResultMap("mep").toString.toFloat
        }
        else {
          null
        })
          ),
        ("sc", (if (leftTympanogramResultMap.getOrElse("sc", null) != null) {
          leftTympanogramResultMap("sc").toString.toFloat
        }
        else {
          null
        })
          ),
        ("volume", (if (leftTympanogramResultMap.getOrElse("vol", null) != null) {
          leftTympanogramResultMap("vol").toString.toFloat
        }
        else {
          null
        })
          ),
        ("trace", leftTympanogramResultMap("trace")
          ),
        ("note", leftTympanogramResultMap("note")
          ),
        ("test_id", tympanogramTestId)
      )

    //Insert into Tympanogram Result
    targetBackend.insertRow(targetTableNames(1), targetTympanogramResultRow, targetDbSchemaName(0))

    try {
      val lastInsertedTympanogramResultIdResult = targetBackend.executeQuery(lastInsertedTympanogramResultIdQuery)
      lastInsertedTympanogramResultIdResult.next()
      tympanogramResultId = lastInsertedTympanogramResultIdResult.getInt("id")
    }
    catch {
      case e: RuntimeException => println("last inserted tympanogram result id could not be retrieved")
      throw e
    }

    etlReferenceRow = DataRow(("resource_id", resourceId(resourceName)),
      ("field_1", "concept.pat_enc_csn_id"),
      ("id_1", pat_enc_csn_id),
      ("target", "staging_tympanogramresult"),
      ("target_id", tympanogramResultId)

    )

    //Insert into staging data source
    targetBackend.insertRow(etlReferenceTableName, etlReferenceRow, etlReferenceDbSchemaName)


    /****  ** ** **    ***/
    //  End Left          //
    /****  ** ** **    ***/
  }


  def saveRightTympanogram(pat_enc_csn_id: String, patientId: Int, encounterId: Int): Unit = {

    /*  Store Date in Database      */
    /*  Insert for Tympanogram Test */
    /*  Insert one Tympanogram test record for each audiogramResultData */


    /****  ** ** **    ***/
    //  Right             //
    /****  ** ** **    ***/


    var tympanogramTestId = 0

    var tympanogramResultId = 0

    val createdDate = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

    val modifiedDate = createdDate

    val lastInsertedTympanogramTestIdQuery: String =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get, targetTableNames(0))


    val lastInsertedTympanogramResultIdQuery: String =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get, targetTableNames(1))


    /**Test        **/
    val targetTympanogramTestRow = DataRow(("created", createdDate),
      ("modified", modifiedDate),
      ("patient_id", patientId),
      ("encounter_id", encounterId)
    )

    //Insert into TympanogramTest
    targetBackend.insertRow(targetTableNames(0), targetTympanogramTestRow, targetDbSchemaName(0))

    try {
      val lastInsertedTympanogramTestIdResult = targetBackend.executeQuery(lastInsertedTympanogramTestIdQuery)
      lastInsertedTympanogramTestIdResult.next()
      tympanogramTestId = lastInsertedTympanogramTestIdResult.getInt("id")
    }
    catch {
      case e: RuntimeException => println("last inserted tympanogram test id could not be retrieved")
      throw e
    }

    var etlReferenceRow = DataRow(("resource_id", resourceId(resourceName)),
      ("field_1", "concept.pat_enc_csn_id"),
      ("id_1", pat_enc_csn_id),
      ("target", "staging_tympanogramtest"),
      ("target_id", tympanogramTestId)

    )

    //Insert into staging data source
    targetBackend.insertRow(etlReferenceTableName, etlReferenceRow, etlReferenceDbSchemaName)


    /**Result      **/

    val targetTympanogramResultRow =

      DataRow(("created", createdDate),
        ("modified", modifiedDate),
        ("ear", "Right"),
        ("probe_freq", (if (rightTympanogramResultMap.getOrElse("frequency", null) != null) {
          rightTympanogramResultMap("frequency").toString.toInt
        }
        else {
          null
        })
          ),
        ("mep", (if (rightTympanogramResultMap.getOrElse("mep", null) != null) {
          rightTympanogramResultMap("mep").toString.toFloat
        }
        else {
          null
        })
          ),
        ("sc", (if (rightTympanogramResultMap.getOrElse("sc", null) != null) {
          rightTympanogramResultMap("sc").toString.toFloat
        }
        else {
          null
        })
          ),
        ("volume", (if (rightTympanogramResultMap.getOrElse("vol", null) != null) {
          rightTympanogramResultMap("vol").toString.toFloat
        }
        else {
          null
        })
          ),
        ("trace", rightTympanogramResultMap("trace")
          ),
        ("note", rightTympanogramResultMap("note")
          ),
        ("test_id", tympanogramTestId)
      )

    //Insert into Tympanogram Result
    targetBackend.insertRow(targetTableNames(1), targetTympanogramResultRow, targetDbSchemaName(0))


    try {
      val lastInsertedTympanogramResultIdResult = targetBackend.executeQuery(lastInsertedTympanogramResultIdQuery)
      lastInsertedTympanogramResultIdResult.next()
      tympanogramResultId = lastInsertedTympanogramResultIdResult.getInt("id")
    }
    catch {
      case e: RuntimeException => println("last inserted tympanogram result id could not be retrieved")
      throw e
    }

    etlReferenceRow = DataRow(("resource_id", resourceId(resourceName)),
      ("field_1", "concept.pat_enc_csn_id"),
      ("id_1", pat_enc_csn_id),
      ("target", "staging_tympanogramresult"),
      ("target_id", tympanogramResultId)

    )

    //Insert into staging data source
    targetBackend.insertRow(etlReferenceTableName, etlReferenceRow, etlReferenceDbSchemaName)


    /****  ** ** **    ***/
    //  End Right         //
    /****  ** ** **    ***/


  }


  def generateSourceEncountersList = {

    val inString =
      (for ((conceptIdName, conceptId) <- conceptIdsMap) yield "'" + conceptId + "'").toString.drop(12).dropRight(1)

    sourceQuery =
      """
      SELECT  DISTINCT c.pat_enc_csn_id
      FROM
      %s.%s c
      WHERE
      concept_id in (%s)
      order by c.pat_enc_csn_id
      """.format(sourceDbSchemaName, sourceTableNames(0), inString)


    try {

      val result = sourceBackend.executeQuery(sourceQuery, DataRow.empty)

      while (result.next()) {

        encountersList += result.getString("pat_enc_csn_id")

      }

    }
    catch {
      case e: java.sql.SQLException =>
        close
        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")
        throw e
    }


  }


  def generateSourceConceptsResultSet(pat_enc_csn_id: String): java.sql.ResultSet = {

    sourceQuery =
      """
      SELECT DISTINCT  c.concept_id as concept_id , c.concept_value as concept_value
      FROM
      %s.%s c
      WHERE
      c.pat_enc_csn_id  = '%s'
      ORDER BY c.concept_id  ASC
      """.format(sourceDbSchemaName(0).get, sourceTableNames(0), pat_enc_csn_id)



    try {

      val result = sourceBackend.executeQuery(sourceQuery, DataRow.empty)

      result
    }
    catch {
      case e: java.sql.SQLException =>
        close
        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")
        throw e
    }

  }

  def retrieveEncounterIdPatientIdByPECId(pat_enc_csn_id: String): MutableList[Int] = {
    //Get the staged encounter id and patient id by data sourced pat_enc_csn_id

    val target = "staging_encounter"

    val field1 = "encounter.pat_enc_csn_id"

    sourceQuery =
      """
      SELECT e.id, e.patient_id
      FROM
      %s.%s  e, %s.%s cd
      WHERE cd.target_id = e.id   AND
      cd.id_1 = '%s'              AND
      cd.field_1 = '%s'           AND
      cd.target = '%s'
      """
        .format(targetDbSchemaName(0).get, sourceTableNames(1), targetDbSchemaName(0).get, sourceTableNames(2), pat_enc_csn_id, field1, target)


    try {

      val queryResult = processBackend.executeQuery(sourceQuery, DataRow.empty)
      if (!queryResult.next) {
        println("Data Sourced patient id and encouner id was not found for encounter " + pat_enc_csn_id)
        sys.exit()
      }
      var result: MutableList[Int] = MutableList.empty
      result += queryResult.getInt("id")
      result += queryResult.getInt("patient_id")


      result
    }
    catch {
      case e: java.sql.SQLException =>
        processBackend.close()
        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")
        throw e
    }

  }

  def parseTrace(conceptValue: String): String = {

    val complianceRange = ordinalToCompliance(asciiToOrdinal(conceptValue.trim.toList.slice(0, 120)))

    pressureRange.zip(complianceRange).toString.drop(7).dropRight(1).mkString("[", "", "]")

  }

  def ordinalToCompliance(value: List[Int]): List[BigDecimal] = {

    val a: List[BigDecimal] = value.map(i => BigDecimal((i - 100)) * BigDecimal(0.01.toString))

    a.map(i => i.setScale(2, BigDecimal.RoundingMode.HALF_UP))

  }

  def asciiToOrdinal(value: List[Char]): List[Int] = {

    value.map(i => i.toInt)

  }

  def resetLeftTympanogramResultMap: Boolean = {

    leftTympanogramResultMap("frequency") = null
    leftTympanogramResultMap("mep") = null
    leftTympanogramResultMap("sc") = null
    leftTympanogramResultMap("vol") = null
    leftTympanogramResultMap("note") = ""
    leftTympanogramResultMap("trace") = ""
    leftTympanogramResultMap("comment") = MutableList.empty

    true
  }

  def resetRightTympanogramResultMap: Boolean = {


    rightTympanogramResultMap("frequency") = null
    rightTympanogramResultMap("mep") = null
    rightTympanogramResultMap("sc") = null
    rightTympanogramResultMap("vol") = null
    rightTympanogramResultMap("note") = ""
    rightTympanogramResultMap("trace") = ""
    rightTympanogramResultMap("comment") = MutableList.empty

    true
  }


}


object TympanogramTransform {

  var defaultSourceTableNames: Seq[String] = Seq("concept_aggregate", "staging_encounter", "core_datasource")

  var defaultTargetTableNames: Seq[String] = Seq("staging_tympanogramtest", "staging_tympanogramresult")

  var defaultSourceSchemaName = "qe11b"

  var defaultTargetSchemaName = "qe11c"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/transform-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/transform-target.properties"


  def apply(): AudiogramTransform = {

    try {

      new AudiogramTransform(
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


  def apply(query: String): AudiogramTransform = {

    try {

      new AudiogramTransform(
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

