package edu.chop.cbmi.etl.audgendb.transform

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 7/13/12
 * Time: 2:31 PM
 * To change this template use File | Settings | File Templates.
 */

import edu.chop.cbmi.etl.transform.sql.SqlTransform

import scala.collection.mutable.Map
import scala.collection.mutable.MutableList

import edu.chop.cbmi.dataExpress.dataModels.DataRow

//import com.etsy.statsd.StatsdClient

case class AudgendbSqlTransform  (  override val sdbfp: String,
                                    override val tdbfp: String,
                                    override val sourceTableNames:    Seq[String],
                                    override val targetTableNames:    Seq[String],
                                    override val sourceDbSchemaName:  Seq[Option[String]],
                                    override val targetDbSchemaName:  Seq[Option[String]],
                                    override val query: Option[String])
  extends SqlTransform(sdbfp,tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {


  val resourceName                          =     "Epic"
  //Could make this var so it would be settable by the user
  val etlReferenceTableName                 =     "core_datasource"

  val etlResourceTableName                  =     "core_resource"

  var etlReferenceDbSchemaName              =     targetDbSchemaName(0)

  val icd9AllowedTableName                  =     "audgendb_icd_allowed"

  val cptAllowedTableName                   =     "audgendb_cpt_allowed"

  val resourceId: Map[String,Int]           =     Map {
                                                        "Epic"  ->  0
                                                  }

  var mode                                  =     "append"


  override def transform():  Boolean   =  {

    if        (mode  ==  "append") {   append  }
    else  if  (mode  ==  "update") {   update  }
    else  { false }

  }

  def append():Boolean    =   {

    false

  }

  def update():Boolean  = {

    false

  }

  def retrieveResourceId(resourceName:  String):  Int  = {

    sourceQuery                                       =
      "SELECT id FROM %s.%s WHERE name = '%s'".format (targetDbSchemaName(0).get,etlResourceTableName,resourceName)



    val resourceIdResult  =   targetBackend.executeQuery(sourceQuery)

    resourceIdResult.next()

    resourceIdResult.getInt("id")


  }

  def retrieveICD9Ids: Map[String, Int]   =   {

    val dbSchemaName    =   targetDbSchemaName(0)

    val tableName       =   sourceTableNames(3)     //Should be staging_diagnosis

    processQuery        =
      """
      SELECT icd9, id
      FROM
      %s.%s
      WHERE
      icd9 is not null
      """.format (dbSchemaName.get,tableName)


    try   {

      val queryResult                           =   processBackend.executeQuery(processQuery,DataRow.empty)

      val icd9Ids:Map[String,Int]               =   Map.empty

      while (queryResult.next())  {
        icd9Ids(queryResult.getString("icd9"))  += queryResult.getInt("id")
      }


      icd9Ids

    }
    catch {
      case e:java.sql.SQLException  =>

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
    }

  }


  def retrieveEthnicities: List[String] = {

    val dbSchemaName    =   sourceDbSchemaName(0)

    val tableName       =   sourceTableNames(1)

    val query           =   "SELECT name FROM %s.%s".format (dbSchemaName.get,tableName)


    try {
      val result: MutableList[String]           =   new MutableList[String]
      val queryResult                           =   sourceBackend.executeQuery(query,DataRow.empty)
      while (queryResult.next())  {
        result += queryResult.getString("name")
      }

      result.toList
    }
    catch {
      case e:java.sql.SQLException  =>

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
    }

  }

  def retrievePatientIdBy_pat_id(pat_id: String): Option[Int]  = {

    val dbSchemaName    =   targetDbSchemaName(0).get     // In General the Target is staging database for a transform

    val tableName       =   etlReferenceTableName         // defaulted to staging_patientphi

    val stagingPatientTableName   =
      "staging_patient"

    val stagingField_1            =
      "patient.pat_id"

    processQuery        =
      """
      SELECT target_id as patient_id FROM %s.%s WHERE id_1 = '%s'  and field_1 = '%s' and target = '%s'
      """.format (dbSchemaName,tableName,pat_id,stagingField_1,stagingPatientTableName)


    try {

      val queryResult     =   processBackend.executeQuery(processQuery,DataRow.empty)

      if (!queryResult.next)  {

        log.write("A staged patient id was not found for pat_id " + pat_id)

        None

      }

      Option(queryResult.getInt("patient_id"))

    }
    catch {
      case e:java.sql.SQLException  =>

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
    }

  }

  //Variations for flexibility
  def retrievePatientIdByMrn(mrn: String): Option[Int] = {

    val dbSchemaName    =   targetDbSchemaName(0).get   // In General the Target is staging database for a transform

    val tableName       =   sourceTableNames(2)         // defaulted to staging_patientphi

    val query           =   "SELECT patient_id FROM %s.%s WHERE mrn = '%s'".format (dbSchemaName,tableName,mrn)


    try {

      val queryResult     =   processBackend.executeQuery(query,DataRow.empty)

      if (!queryResult.next){

        log.write("A staged patient id was not found for mrn " + mrn)

        return None

      }

      Option(queryResult.getInt("patient_id"))

    }
    catch {
      case e:java.sql.SQLException  =>

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e

    }

  }

  def generateSourceResultSet():    Boolean     =       {   false   }

  def generateSourceResultSet( element:Option[String]  = None, ids:Option[Seq[Int]]  = None):
  Boolean    =     {   false   }

  def generateSourceResultSet(element:String):
  Boolean    =     {   false   }

  def generateSourceResultSet(ids: Int*):
  Boolean    =     {   false   }
  //Variations for flexibility
  def buildSourceQuery( element:Option[String]  = None, ids:Option[Seq[Int]]  = None):
  String    =     {     null   }

  def buildSourceQuery(element:String):
  String    =     {     null   }

  def buildSourceQuery(ids: Seq[Int]):
  String    =     {     null   }

  override def tryConnection():  java.sql.ResultSet  = {
    /*  This is really added so that the sourceResult can be instantiated at object construction time       */
    /*  and then accessed throughout the different functions                                                */

    /*  This may need to be overridden if the source DBMS type is changed from postgres                     */

    sourceQuery                                            =
      "SELECT (1) FROM %s.%s LIMIT 1 ".format(sourceDbSchemaName(0).get,sourceTableNames(0))



    sourceBackend.executeQuery(sourceQuery)

  }


}
