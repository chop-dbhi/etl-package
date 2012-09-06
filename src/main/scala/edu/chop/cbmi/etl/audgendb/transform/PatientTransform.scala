package edu.chop.cbmi.etl.audgendb.transform

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */

import edu.chop.cbmi.etl.transform.sql.SqlTransform
import edu.chop.cbmi.etl.util.FileProperties

import edu.chop.cbmi.dataExpress.dataModels.DataRow
import edu.chop.cbmi.dataExpress.backends.SqlBackendFactory

import scala.collection.mutable.MutableList
import scala.util.Random
import scala.util.control.Breaks._


import java.util.Calendar




case class PatientTransform(  override val sdbfp: String,                             override val tdbfp: String,
                              override val sourceTableNames:    Seq[String],          override val targetTableNames:    Seq[String],
                              override val sourceDbSchemaName:  Seq[Option[String]],  override val targetDbSchemaName:  Seq[Option[String]],
                              override val query: Option[String])
  extends AudgendbSqlTransform(sdbfp,tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {


  val ethnicities: List[String]             = retrieveEthnicities

  val randomNumberGenerator                 = new Random

  override def append:  Boolean  =  {
    /* Transform each row(s) of data for the entire table(s)*/

    resourceId(resourceName)                        =   retrieveResourceId(resourceName)

    var ethnicity: String                           =   ""

    var deathDate: java.lang.Object                 =   null

    var birthDate: java.lang.Object                 =   null

    var sex: String                                 =   null

    var middleName:  String                         =   null

    val patientDateFormat                           =   new java.text.SimpleDateFormat("yyyy-MM-dd")

    generateSourceResultSet(None,None)

    var targetPatientRow: DataRow[Any]              =   DataRow.empty

    var targetPatientPhiRow: DataRow[Any]           =   DataRow.empty

    var etlReferenceRow: DataRow[Any]               =   DataRow.empty

    var patientId                                   =   0

    var patientPhiId                                =   0

    val lastInsertedPatientIdQuery: String          =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get,targetTableNames(0))

    val lastInsertedPatientPhiIdQuery: String       =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get,targetTableNames(1))

    targetBackend.connection.setAutoCommit(false)

    //targetBackend.commit

    while (sourceResult.next())   {

      val createdDate                 =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

      val modifiedDate                =   createdDate

      /** Ethnicity Transform **/
      if ((sourceResult.getString("ethnic_group_name") != null)  && (sourceResult.getString("ethnic_group_name") != "null"))
      {
        //This transform is not achieving anything at the moment
        //The python scripts don't show a real transformation for ethnicity but a "transform"
        //is scripted there.  This is being over stepped for now
        //
        //ethnicity = ethnicityTransform(sourceResult.getString("ethnic_group_name"))
        //
        ethnicity = sourceResult.getString("ethnic_group_name")
      }
      else ethnicity  = null

      /** The Death Date is not being stored in production currently.   **/
      /**

      if ((sourceResult.getString("death_date") != null) && (sourceResult.getString("death_date") != "null"))
      {
        deathDate       = patientDateFormat.parse(sourceResult.getString("death_date") )
      }
      else deathDate  = null

       **/
      /*******************************************************************/

      /** Birth Date Transform          **/
      if ((sourceResult.getString("birth_date") != null) && (sourceResult.getString("birth_date") != "null"))
      {
        birthDate       = patientDateFormat.parse(sourceResult.getString("birth_date") )
      }
      else birthDate  = null
      /**                               **/
      
      /** Sex (Gender) Date Transform   **/
      if        ((sourceResult.getString("sex").trim.toLowerCase == "m"))         {   sex   = "Male"      }
      else  if  ((sourceResult.getString("sex").trim.toLowerCase == "f"))         {   sex   = "Female"    }
      else                                                                        {   sex   = null        }
      /**                               **/

      /** Middle Name Transform         **/

      middleName  =   sourceResult.getString("pat_middle_name")

      if        (( middleName ==  null)  ||  ( middleName.toUpperCase ==  "NULL"))  {   middleName   =  null      }
      else      ( middleName  = sourceResult.getString("pat_middle_name").toUpperCase )

      /**                               **/

      val patientAlias      = generatePatientAlias
      
      if (!patientAlias.startsWith("P"))  {

        //Could Not Generate Brand New Alias
        //Print Message and Exit

        println(patientAlias)
        
        System.exit(1)
      }
      //Id column should be auto-generated
      targetPatientRow                          =   DataRow(  ("created",createdDate),
                                                              ("modified",modifiedDate),
                                                              ("sex",sex),
                                                              ("alias",patientAlias),
                                                              ("ethnicity",ethnicity)
                                                    )


      targetBackend.insertRow(targetTableNames(0),targetPatientRow,targetDbSchemaName(0))


      try {
        val lastInsertedPatientIdResult           =   targetBackend.executeQuery(lastInsertedPatientIdQuery)
        lastInsertedPatientIdResult.next()
        patientId                                 =   lastInsertedPatientIdResult.getInt("id")
      }  
      catch {
        case e:RuntimeException        => println("last inserted patient id could not be retrieved")
                                          throw e
      }


      etlReferenceRow                          =    DataRow(  ("resource_id",resourceId(resourceName)),
                                                              ("field_1","patient.pat_id"),
                                                              ("id_1",sourceResult.getObject("pat_id")),
                                                              ("target","staging_patient") ,
                                                              ("target_id",patientId),
                                                              ("md5sum",sourceResult.getString("md5"))
                                                    )

      targetBackend.insertRow(etlReferenceTableName,etlReferenceRow, etlReferenceDbSchemaName)

      targetPatientPhiRow                     =     DataRow(  ("created",createdDate),
                                                              ("modified",modifiedDate),
                                                              ("first_name",sourceResult.getString("pat_first_name").toUpperCase),
                                                              ("middle_name",middleName),
                                                              ("last_name",sourceResult.getString("pat_last_name").toUpperCase),
                                                              ("patient_id",patientId),
                                                              ("mrn",sourceResult.getObject("pat_mrn_id")),
                                                              ("dob",birthDate),
                                                              //("dod",deathDate),
                                                              ("zipcode",sourceResult.getString("zip"))
                                                    )



      targetBackend.insertRow(targetTableNames(1),targetPatientPhiRow,targetDbSchemaName(0))


      try {
        val lastInsertedPatientPhiIdResult              =   targetBackend.executeQuery(lastInsertedPatientPhiIdQuery)
        lastInsertedPatientPhiIdResult.next()
        patientPhiId                                    =   lastInsertedPatientPhiIdResult.getInt("id")
      }
      catch {
        case e:RuntimeException        => println("last inserted patient id could not be retrieved")

        close

        throw e
      }

      etlReferenceRow                          =    DataRow(  ("resource_id",resourceId(resourceName)),
                                                              ("field_1","patient.pat_id"),
                                                              ("id_1",sourceResult.getObject("pat_id")),
                                                              ("target","staging_patientphi") ,
                                                              ("target_id",patientPhiId),
                                                              ("md5sum",sourceResult.getString("md5"))
                                                    )

      targetBackend.insertRow(etlReferenceTableName,etlReferenceRow, etlReferenceDbSchemaName)

      //Only Commit if All Inserts are successful
      targetBackend.commit()

    }


  close

  true

  }

  override def generateSourceResultSet(element:Option[String]  = None, ids:Option[Seq[Int]] = None): Boolean  =   {

    val query           = "SELECT *  FROM %s.%s".format (sourceDbSchemaName(0).get,sourceTableNames(0))

    try {

      sourceResult     =   sourceBackend.executeQuery(query,DataRow.empty)

      return true
    }
    catch {
      case e:java.sql.SQLException  =>

            close

            println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

            throw e
    }


  }

  def ethnicityTransform( data: String): String  = {
    /* Transform the ethnicity for one data input */
    var ethnicity: String     = ""

    breakable {
      for(value: String <- ethnicities){

        if (data == value) {
          ethnicity =   value
          break
        }
      }
    }

    ethnicity
  }

  def generatePatientAlias: String    = {

    val maxAliasNumber                              =   10 * 10 * 10 * 10 * 10 * 10 - 1

    val minAliasNumber                              =   10 * 10 * 10 * 10 * 10

    val aliasNumberRange                            =   minAliasNumber to maxAliasNumber

    val maxAliasGenerationAttempts: Int             =   20

    val existingPatientAliases: MutableList[Any]    =   MutableList.empty

    val existingPatientAliasesQuery: String         =
      "SELECT alias FROM %s.%s".format (targetDbSchemaName(0).get,targetTableNames(0))


    for (i <- 1 to maxAliasGenerationAttempts) {

      val patientAlias    = "P" + aliasNumberRange(randomNumberGenerator.nextInt(aliasNumberRange.length)).toString

      val aliasResult = targetBackend.executeQuery(existingPatientAliasesQuery)

      while(aliasResult.next()) {

        existingPatientAliases  +=  aliasResult.getString("alias")

      }

      if (!existingPatientAliases.contains(patientAlias)) { return patientAlias }


    }


    "Could not generate a brand new alias after " + maxAliasGenerationAttempts + " tries "

  }
  
  override def close:  Boolean  = {

    log.close()

    sourceBackend.close()

    targetBackend.close()

    true
  }

}


object PatientTransform  {

  var defaultSourceTableNames: Seq[String]      = Seq("patient","ethnicity")

  var defaultTargetTableNames: Seq[String]      = Seq("staging_patient","staging_patientphi")

  var defaultSourceSchemaName                   = "qe11b"

  var defaultTargetSchemaName                   = "qe11c"

  val defaultSourcePropertiesFP                 =
    "conf/connection-properties/transform-source.properties"

  val defaultTargetPropertiesFP                 =
    "conf/connection-properties/transform-target.properties"


  def apply(): PatientTransform   =   {

    try {

      new PatientTransform  (
        defaultSourcePropertiesFP,            defaultTargetPropertiesFP,
        defaultSourceTableNames,              defaultTargetTableNames,
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        None
      )
    }
    catch {
      case e:RuntimeException     => throw new RuntimeException
    }

  }



  def apply(query: String): PatientTransform  =   {

    try {

      new PatientTransform  (
        defaultSourcePropertiesFP,            defaultTargetPropertiesFP,
        defaultSourceTableNames,              defaultTargetTableNames,
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(query)
      )
    }
    catch {
      case e:RuntimeException     => throw new RuntimeException
    }

  }




}

