package edu.chop.cbmi.etl.audgendb.transform

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */


import edu.chop.cbmi.etl.library.date.DateCleaner


import edu.chop.cbmi.etl.audgendb.extraction.MedicalHistoryExtraction
import edu.chop.cbmi.etl.audgendb.extraction.EncounterDiagnosisExtraction
import edu.chop.cbmi.etl.audgendb.extraction.ProblemListExtraction

import scala.collection.mutable.MutableList

import edu.chop.cbmi.dataExpress.dataModels.DataRow

import java.util.Calendar



case class ProcedureTransform(  override val sdbfp: String,                             override val tdbfp: String,
                                override val sourceTableNames:  Seq[String],            override val targetTableNames:  Seq[String],
                                override val sourceDbSchemaName:  Seq[Option[String]],  override val targetDbSchemaName:  Seq[Option[String]],
                                override val query: Option[String])
  extends AudgendbSqlTransform(sdbfp,tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {

  
  val DateCleaner                                                         =   new DateCleaner

  //Used to skip redundant code-visit_dates for a patient
  var patientBuffer: MutableList[(Int,java.util.Date, Int)]               =   MutableList.empty

  processBackend.connect()

  val elements:Seq[String]                                    =
    Seq("procedure","surgical_history")

  //TODO
  //Append needs to be broken up into elements
  override def append:  Boolean  =  {
    /* Transform each row(s) of data for the entire table(s)*/

    resourceId(resourceName)                                      =   retrieveResourceId(resourceName)

    var procedureDetailRow: DataRow[Any]                          =   DataRow.empty

    var etlReferenceRow: DataRow[Any]                             =   DataRow.empty

    var procedureDetailId                                        =   0

    //Used to skip redundant code-visit_dates for a patient
    var patientBuffer: MutableList[(Int,java.util.Date, Int)]     =   MutableList.empty
    
    var visitDate                                                 =   new java.util.Date

    var createdDate                                               =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

    var modifiedDate                                              =   createdDate

    var source: String                                            =   null

    var patientId:Int                                             =   0

    var procedureId:Int                                           =   0

    processQuery          =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get,targetTableNames(0))

    targetBackend.connection.setAutoCommit(false)

    elements.foreach    {   element =>

      generateSourceResultSet(element)                        //New Patients only

      while (sourceResult.next())   {

        createdDate                 =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime())

        modifiedDate                =   createdDate

        visitDate                   =   if (element == "medical_history") {   //Medical History uses Varchar

                                          DateCleaner.clean(sourceResult.getString("noted_date")).getOrElse(null)

                                        }
                                        else                              {

                                          sourceResult.getDate("noted_date")

                                        }

        source                      =   sourceResult.getString("source")

        patientId                   =   sourceResult.getInt("patient_id")

        procedureId                 =   sourceResult.getInt("procedure_id")

        //Id column should be auto-generated
        procedureDetailRow                    =     DataRow(    ("created",createdDate),
                                                                ("modified",modifiedDate),
                                                                ("visit_date",visitDate) ,
                                                                ("source",source),
                                                                ("patient_id",patientId),
                                                                ("procedure_id" , procedureId)
                                                    )
        //Clear the patient Buffer this is a new patient
        if (patientBuffer(0)._1 !=  patientId)  { patientBuffer = MutableList.empty}
        //If this patientid,visitDate, and code is not already stored insert and store it in the buffer
        if (!patientBuffer.contains((patientId,visitDate,procedureId)))    {

          targetBackend.insertRow(targetTableNames(0),procedureDetailRow ,targetDbSchemaName(0)   )

          try {
            val result                =   targetBackend.executeQuery(processQuery)
            result.next()
            procedureDetailId        =   result.getInt("id")
          }
          catch {
            case e:RuntimeException        => println("last inserted patient id could not be retrieved")
            throw e
          }


          etlReferenceRow             =    DataRow(   ("resource_id",resourceId(resourceName)),
                                                      ("field_1","problem_list.icd9_code"),
                                                      ("id_1",sourceResult.getObject("pat_id")),
                                                      ("field_2","medical_hx.icd9_code"),
                                                      ("id_2",sourceResult.getObject("pat_id")),
                                                      ("field_3","pat_enc_dx.icd9_code"),
                                                      ("id_3",sourceResult.getObject("pat_id")),
                                                      ("target","staging_proceduredetail") ,
                                                      ("target_id",procedureDetailId),
                                                      ("extract_date",sourceResult.getDate("extract_date")),
                                                      ("md5sum",sourceResult.getString("md5"))
                                            )

          targetBackend.insertRow(etlReferenceTableName,etlReferenceRow, etlReferenceDbSchemaName)

          patientBuffer +=      ((patientId,visitDate,procedureId))

        }

        //Only Commit if All Inserts are successful
        targetBackend.commit()

      }

      sourceResult.close()

    }

  true

  }

  override def update():Boolean  = {
    //New Patients only
    resourceId(resourceName)                                      =   retrieveResourceId(resourceName)

    var procedureDetailRow: DataRow[Any]                          =   DataRow.empty

    var etlReferenceRow: DataRow[Any]                             =   DataRow.empty

    var procedureDetailId                                         =   0

    //Used to skip redundant code-visit_dates for a patient
    var patientBuffer: MutableList[(Int,java.util.Date, Int)]     =   MutableList.empty

    var visitDate                                                 =   new java.util.Date

    var createdDate                                               =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

    var modifiedDate                                              =   createdDate

    var source: String                                            =   null

    var patientId:Int                                             =   0

    var procedureId:Int                                           =   0

    processQuery          =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get,targetTableNames(0))

    targetBackend.connection.setAutoCommit(false)

    elements.foreach    {   element =>

      generateSourceResultSet(element)

      while (sourceResult.next())   {

        createdDate                 =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime())

        modifiedDate                =   createdDate

        visitDate                   =   if (element == "surgical_history") {   //Medical History uses Varchar

                                          DateCleaner.clean(sourceResult.getString("visit_date")).getOrElse(null)

                                        }
                                        else                              {

                                          sourceResult.getDate("visit_date")

                                        }

        source                      =   sourceResult.getString("source")

        patientId                   =   sourceResult.getInt("patient_id")

        procedureId                 =   sourceResult.getInt("procedure_id")

        //Id column should be auto-generated
        procedureDetailRow                    =     DataRow(    ("created",createdDate),
                                                                ("modified",modifiedDate),
                                                                ("visit_date",visitDate) ,
                                                                ("source",source),
                                                                ("patient_id",patientId),
                                                                ("procedure_id" , procedureId)
                                                   )
        //Clear the patient Buffer this is a new patient
        if ((patientBuffer.length > 0) )     {

          if (patientBuffer(0)._1 !=  patientId)  { patientBuffer = MutableList.empty}

        }

        if (!patientBuffer.contains((patientId,visitDate,procedureId)))    {
          //This patientid,visitDate, and code is not already stored so insert and store it in the buffer
          targetBackend.insertRow(targetTableNames(0),procedureDetailRow ,targetDbSchemaName(0)   )

          try {
            val result                =   targetBackend.executeQuery(processQuery)
            result.next()
            procedureDetailId         =   result.getInt("id")
          }
          catch {
            case e:RuntimeException        => println("last inserted patient id could not be retrieved")
            throw e
          }

          val field_1 = if        (element == "surgical_history")   { "surgical_hx.cpt_code"  }
                        else  if  (element == "procedure")          { "procedure.proc_code"   }
                        else  {

                                println("Bad field_1 for insert")
                                //Maybe should log as well
                                throw new RuntimeException
                        }

          val id_1    = if        (element == "surgical_history")   { sourceResult.getObject("cpt_code")  }
                        else  if  (element == "procedure")          { sourceResult.getObject("proc_code")   }
                        else  {

                                println("Bad id_1 for insert")
                                //Maybe should log as well
                                throw new RuntimeException

                        }



            etlReferenceRow             =    DataRow(   ("resource_id",resourceId(resourceName)),
                                                        ("field_1",field_1),
                                                        ("id_1",id_1),
                                                        ("target","staging_proceduredetail") ,
                                                        ("target_id",procedureDetailId),
                                                        ("extract_date",sourceResult.getDate("extract_date")),
                                                        ("md5sum",sourceResult.getString("md5"))
                                           )

          targetBackend.insertRow(etlReferenceTableName,etlReferenceRow, etlReferenceDbSchemaName)

          patientBuffer +=      ((patientId,visitDate,procedureId))

        }

        //Only Commit if All Inserts are successful
        targetBackend.commit()

      }

      sourceResult.close()

    }

    true


  }

  override def generateSourceResultSet(element:String): Boolean = {

    sourceQuery                      =    buildSourceQuery(element)

    try {

      val startTime                   =   Calendar.getInstance.getTimeInMillis/1000   //Seconds

      sourceResult                    =   sourceBackend.executeQuery(sourceQuery,DataRow.empty)

      return true
    }
    catch {
      case e:java.sql.SQLException  =>

            log.write("--SOURCE QUERY--\n\n%s".format(sourceQuery))

            close

            println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

            throw e
    }

  }

  override def buildSourceQuery(element:String):String                =   {

    val proceduresTableName               =   sourceTableNames(0)

    val surgicalHistoryTableName          =   sourceTableNames(1)

    //Modify this query to select for patients 'not in' staging
    val stagingProcedureTableName         =   sourceTableNames(2)

    val stagingProcedureDetailTableName   =   targetTableNames(0)

    val stagingPatientTableName           =   "staging_patient"

    val field_1                           =   "patient.pat_id"



    //Ordering by patient_id is important because there is a buffer being used to filter out duplicate
    //Code-Noted_date for each patient
    //Results from 3 tables are UNIONED together
    //Using in or not in datasource clause to select between existing(update) or new patients(append)
    //To do a complete refresh, do append and then an update

    lazy  val procedureUpdateQuery   =
      """
      select
      distinct
      c.target_id                                     as "patient_id",
      p."pat_id"                                      as "pat_id",
      p."proc_code"                                   as "proc_code",
      p."orig_service_date"                           as "visit_date",
      'Billing Data'                                  as "source",
      sp."id"                                         as "procedure_id",
      p.extract_date                                  as "extract_date",
      p.md5                                           as "md5"
      FROM
      %s.%s p,       %s.%s c ,   %s.%s sp
      where
      p."pat_id"		  =	c."id_1"				              and
      c."field_1"		  =	'%s'		                      and
      c."target"		  =	'%s'		                      and
      p."proc_code"	  =	sp."code"				              and
      p."proc_code"                                   in
      (select distinct "cpt_code" from %s.%s)         and
      p."proc_code"  is not null
      order by "patient_id" asc
      """.format( sourceDbSchemaName(0).get,proceduresTableName, etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingProcedureTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,cptAllowedTableName

      )

    lazy  val surgicalHistoryUpdateQuery       =
      """
       select
       distinct
       c.target_id                                        as "patient_id",
       sh."pat_id"                                        as "pat_id",
       sh.surgical_hx_date                                as "visit_date",
       sh.cpt_code                                        as "cpt_code",
       sh.proc_code                                       as "proc_code",
       'Medical History'                                  as "source",
       sp."id"                                            as "procedure_id",
       sh.extract_date                                    as "extract_date",
       sh.md5                                             as "md5"
       FROM
       %s.%s sh,       %s.%s c,  %s.%s sp
       where
       sh."pat_id"		  =	c."id_1"				                and
       c."field_1"		  =	'%s'		                        and
       c."target"		    =	'%s'		                        and
       sh."cpt_code"	  =	sp."code"				                and
       sh."cpt_code"                                      in
       (select distinct "cpt_code" from %s.%s)            and
       sh."cpt_code"  is not null
       order by "patient_id" asc
      """.format(  sourceDbSchemaName(0).get,surgicalHistoryTableName,etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingProcedureTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,cptAllowedTableName
      )




    lazy  val procedureAppendQuery   =
      """
      select
      distinct
      c.target_id                                     as "patient_id",
      p."pat_id"                                      as "pat_id",
      p."proc_code"                                   as "proc_code",
      p."orig_service_date"                           as "visit_date",
      'Billing Data'                                  as "source",
      sp."id"                                         as "procedure_id",
      p.extract_date                                  as "extract_date",
      p.md5                                           as "md5"
      FROM
      %s.%s p,       %s.%s c ,   %s.%s sp
      where
      p."pat_id"		  =	c."id_1"				                    and
      c."field_1"		  =	'%s'		                            and
      c."target"		  =	'%s'		                            and
      p."proc_code"	  =	sp."code"				                    and
      p."proc_code"                                         in
      (select distinct "cpt_code" from %s.%s)               and
      p."proc_code"  is not null                            and
      c."target_id"                                         not in
      (select distinct patient_id from %s.%s)
      order by "patient_id" asc
      """.format( sourceDbSchemaName(0).get,proceduresTableName, etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingProcedureTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,cptAllowedTableName,
        targetDbSchemaName(0).get,stagingProcedureDetailTableName
      )



    lazy  val surgicalHistoryAppendQuery       =
      """
       select
       distinct
       c.target_id                                        as "patient_id",
       sh."pat_id"                                        as "pat_id",
       sh.surgical_hx_date                                as "visit_date",
       sh.cpt_code                                        as "cpt_code",
       sh.proc_code                                       as "proc_code",
       'Medical History'                                  as "source",
       sp."id"                                            as "procedure_id",
       sh.extract_date                                    as "extract_date",
       sh.md5                                             as "md5"
       FROM
       %s.%s sh,       %s.%s c,  %s.%s sp
       where
       sh."pat_id"		  =	c."id_1"				                  and
       c."field_1"		  =	'%s'		                          and
       c."target"		    =	'%s'		                          and
       sh."cpt_code"	  =	sp."code"				                  and
       sh."cpt_code"                                        in
       (select distinct "cpt_code" from %s.%s)              and
       sh."cpt_code"  is not null                           and
       c."target_id"                                        not in
       (select distinct patient_id from %s.%s)
       order by "patient_id" asc
      """.format(  sourceDbSchemaName(0).get,surgicalHistoryTableName,etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingProcedureTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,cptAllowedTableName,
        targetDbSchemaName(0).get,stagingProcedureDetailTableName
      )




    if (mode == "update")      {        //Existing Patients

       {

        if (element   == "procedure")    {

          procedureUpdateQuery

        }
        else if (element == "surgical_history")    {

          surgicalHistoryUpdateQuery

        }
        else  {

          println("Bad Operation.  Bad Update Element.  Element Query Name Must be procedure or surgical_history.")

          sys.exit()
        }

      }

    }
    else  if (mode == "append")      {        //New Patients

      {

        if (element      == "procedure")                 {

          procedureAppendQuery

        }
        else if (element  == "surgical_history")          {

          surgicalHistoryAppendQuery

        }
        else  {

          println("Bad Operation.  Bad Update Element.  Element Query Name Must be procedure or surgical_history")

          sys.exit()
        }

      }

    }
    else  {

      println("Bad Operation.  Mode must be update or append")

      sys.exit()
    }

  }

  def performanceIndexes(operation:String = "info"):Boolean   = {

    if (operation == "add")   {

      performanceIndex01("add")

      performanceIndex02("add")

      performanceIndex03("add")

    }
    else  if (operation == "drop")  {

      performanceIndex01("drop")

      performanceIndex02("drop")

      performanceIndex03("drop")

    }
    else  if (operation == "info")  {

      performanceIndex01("info")

      performanceIndex02("info")

      performanceIndex03("info")

    }
    else {

      println("Bad Operation.  Operation must be add,drop, or info")

      return false

    }


  }

  def performanceIndex01(operation:String):Boolean  = {
    //Performance index for adding md5 to medical_history table

    
    if (operation == "add")   {

      val extraction  = MedicalHistoryExtraction()

      extraction.performanceIndex("add")

      extraction.close

    }
    else  if (operation == "drop")  {

      val extraction  = MedicalHistoryExtraction()

      extraction.performanceIndex("drop")

      extraction.close

    }
    else  if (operation == "info")  {
      
      println("performanceIndex01 - table:medical_history")

      true
      
    }
    else {

      println("Bad Operation.  Operation must be add,drop, or info")

      return false

    }
  }

  def performanceIndex02(operation:String):Boolean  = {
    //Performance index for adding md5 to medical_history table


    if (operation == "add")   {

      val extraction  = EncounterDiagnosisExtraction()

      extraction.performanceIndex("add")

      extraction.close

    }
    else  if (operation == "drop")  {

      val extraction  = EncounterDiagnosisExtraction()

      extraction.performanceIndex("drop")

      extraction.close

    }
    else  if (operation == "info")  {

      println("performanceIndex02 - table:encounter_diagnosis")

      true

    }
    else {

      println("Bad Operation.  Operation must be add,drop, or info")

      return false

    }
  }

  def performanceIndex03(operation:String):Boolean  = {
    //Performance index for adding md5 to medical_history table


    if (operation == "add")   {

      val extraction  = ProblemListExtraction()

      extraction.performanceIndex("add")

      extraction.close

    }
    else  if (operation == "drop")  {

      val extraction  = ProblemListExtraction()

      extraction.performanceIndex("drop")

      extraction.close

    }
    else  if (operation == "info")  {

      println("performanceIndex03 - table:problemlist")

      true

    }
    else {

      println("Bad Operation.  Operation must be add,drop, or info")

      return false

    }
  }

  override def close:  Boolean  = {

    log.close()
    processBackend.close()
    sourceBackend.close()
    targetBackend.close()

    true
  }

}


object ProcedureTransform  {

  var defaultSourceTableNames: Seq[String]      = Seq("procedure","surgical_history","staging_procedure")

  var defaultTargetTableNames: Seq[String]      = Seq("staging_proceduredetail")

  var defaultSourceSchemaName                   = "qe11b"

  var defaultTargetSchemaName                   = "qe11c"

  val defaultSourcePropertiesFP                 =
    "conf/connection-properties/transform-source.properties"

  val defaultTargetPropertiesFP                 =
    "conf/connection-properties/transform-target.properties"


  def apply(): ProcedureTransform   =   {

    try {

      new ProcedureTransform  (
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



  def apply(query: String): ProcedureTransform  =   {

    try {

      new ProcedureTransform  (
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

