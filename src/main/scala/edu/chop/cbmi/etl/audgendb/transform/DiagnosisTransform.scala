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



case class DiagnosisTransform(  override val sdbfp: String,                             override val tdbfp: String,
                                override val sourceTableNames:  Seq[String],            override val targetTableNames:  Seq[String],
                                override val sourceDbSchemaName:  Seq[Option[String]],  override val targetDbSchemaName:  Seq[Option[String]],
                                override val query: Option[String])
  extends AudgendbSqlTransform(sdbfp,tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {

  
  val DateCleaner                                                         =   new DateCleaner

  //Used to skip redundant code-visit_dates for a patient
  var patientBuffer: MutableList[(Int,java.util.Date, Int)]               =   MutableList.empty

  processBackend.connect()

  val updateSourceElements:Seq[String]                                    =
    Seq("encounter_diagnosis","medical_history","problemlist")
  //Append is not broken into elements at the moment
  val appendSourceElements:Seq[String]                                    =   Seq.empty

  //TODO
  //Append needs to be broken up into elements
  override def append:  Boolean  =  {
    /* Transform each row(s) of data for the entire table(s)*/

    resourceId(resourceName)                                      =   retrieveResourceId(resourceName)

    var diagnosisDetailRow: DataRow[Any]                          =   DataRow.empty

    var etlReferenceRow: DataRow[Any]                             =   DataRow.empty

    var diagnsosisDetailId                                        =   0

    //Used to skip redundant code-visit_dates for a patient
    var patientBuffer: MutableList[(Int,java.util.Date, Int)]     =   MutableList.empty
    
    var visitDate                                                 =   new java.util.Date

    var createdDate                                               =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

    var modifiedDate                                              =   createdDate

    var source: String                                            =   null

    var patientId:Int                                             =   0

    var diagnosisId:Int                                           =   0

    processQuery          =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get,targetTableNames(0))

    targetBackend.connection.setAutoCommit(false)

    appendSourceElements.foreach  {   element =>

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

        diagnosisId                 =   sourceResult.getInt("diagnosis_id")

        //Id column should be auto-generated
        diagnosisDetailRow                    =     DataRow(    ("created",createdDate),
                                                                ("modified",modifiedDate),
                                                                ("visit_date",visitDate) ,
                                                                ("source",source),
                                                                ("patient_id",patientId),
                                                                ("diagnosis_id" , diagnosisId)
                                                    )
        //Clear the patient Buffer this is a new patient
        if (patientBuffer(0)._1 !=  patientId)  { patientBuffer = MutableList.empty}
        //If this patientid,visitDate, and code is not already stored insert and store it in the buffer
        if (!patientBuffer.contains((patientId,visitDate,diagnosisId)))    {

          targetBackend.insertRow(targetTableNames(0),diagnosisDetailRow ,targetDbSchemaName(0)   )

          try {
            val result                =   targetBackend.executeQuery(processQuery)
            result.next()
            diagnsosisDetailId        =   result.getInt("id")
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
                                                      ("target","staging_diagnosisdetail") ,
                                                      ("target_id",diagnsosisDetailId),
                                                      ("extract_date",sourceResult.getDate("extract_date")),
                                                      ("md5sum",sourceResult.getString("md5"))
                                            )

          targetBackend.insertRow(etlReferenceTableName,etlReferenceRow, etlReferenceDbSchemaName)

          patientBuffer +=      ((patientId,visitDate,diagnosisId))

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

    var diagnosisDetailRow: DataRow[Any]                          =   DataRow.empty

    var etlReferenceRow: DataRow[Any]                             =   DataRow.empty

    var diagnsosisDetailId                                        =   0

    //Used to skip redundant code-visit_dates for a patient
    var patientBuffer: MutableList[(Int,java.util.Date, Int)]     =   MutableList.empty

    var visitDate                                                 =   new java.util.Date

    var createdDate                                               =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

    var modifiedDate                                              =   createdDate

    var source: String                                            =   null

    var patientId:Int                                             =   0

    var diagnosisId:Int                                           =   0

    processQuery          =
      "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get,targetTableNames(0))

    targetBackend.connection.setAutoCommit(false)

    updateSourceElements.foreach  {   element =>

      generateSourceResultSet(Option(element),None)

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

        diagnosisId                 =   sourceResult.getInt("diagnosis_id")

        //Id column should be auto-generated
        diagnosisDetailRow                    =     DataRow(    ("created",createdDate),
                                                                ("modified",modifiedDate),
                                                                ("visit_date",visitDate) ,
                                                                ("source",source),
                                                                ("patient_id",patientId),
                                                                ("diagnosis_id" , diagnosisId)
                                                   )
        //Clear the patient Buffer this is a new patient
        if ((patientBuffer.length > 0) )     {

          if (patientBuffer(0)._1 !=  patientId)  { patientBuffer = MutableList.empty}

        }

        if (!patientBuffer.contains((patientId,visitDate,diagnosisId)))    {
          //This patientid,visitDate, and code is not already stored so insert and store it in the buffer
          targetBackend.insertRow(targetTableNames(0),diagnosisDetailRow ,targetDbSchemaName(0)   )

          try {
            val result                =   targetBackend.executeQuery(processQuery)
            result.next()
            diagnsosisDetailId        =   result.getInt("id")
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
                                                      ("target","staging_diagnosisdetail") ,
                                                      ("target_id",diagnsosisDetailId),
                                                      ("extract_date",sourceResult.getDate("extract_date")),
                                                      ("md5sum",sourceResult.getString("md5"))
                                           )

          targetBackend.insertRow(etlReferenceTableName,etlReferenceRow, etlReferenceDbSchemaName)

          patientBuffer +=      ((patientId,visitDate,diagnosisId))

        }

        //Only Commit if All Inserts are successful
        targetBackend.commit()

      }

      sourceResult.close()

    }

    true


  }

  override def generateSourceResultSet(element:Option[String], ids:Option[Seq[Int]]): Boolean = {

    sourceQuery                      =   buildSourceQuery(element,None)

    try {

      val startTime   =   Calendar.getInstance.getTimeInMillis/1000   //Seconds

      sourceResult                    =   sourceBackend.executeQuery(sourceQuery,DataRow.empty)

      //statsClient.timing( "edu.chop.cbmi.etl.audgendb.transform.DiagnosisTransform.sourceQuery.execution.timer",
      //                    Calendar.getInstance.getTimeInMillis/1000 - startTime
      //)

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

  override def buildSourceQuery(element:Option[String],ids:Option[Seq[Int]] = None):String              =  {

    val problemListTableName          =   sourceTableNames(0)

    val encounterDiagnosisTableName   =   sourceTableNames(1)

    val medicalHistoryTableName       =   sourceTableNames(2)
    //Modify this query to select for patients 'not in' staging
    val stagingDiagnosisTableName     =   sourceTableNames(3)

    val stagingPatientTableName       =   "staging_patient"

    val field_1                       =   "patient.pat_id"
    
    val pat_enc_csn_id                =   if (!ids.isEmpty) { ids.get(0) } else null


    //Ordering by patient_id is important because there is a buffer being used to filter out duplicate
    //Code-Noted_date for each patient
    //Results from 3 tables are UNIONED together
    //Using in or not in datasource clause to select between existing(update) or new patients(append)
    //To do a complete refresh, do append and then an update

    lazy  val encounterDiagnosisUpdateQuery   =
      """
      select
      distinct
      c.target_id                                     as "patient_id",
      ed."pat_id"                                     as "pat_id",
      ed."icd9_code"                                  as "icd9_code",
      ed.contact_date                                 as "noted_date",
      'Billing Data'                                  as "source",
      d."id"                                          as "diagnosis_id",
      ed.extract_date                                 as "extract_date",
      ed.md5                                          as "md5"
      FROM
      %s.%s ed,       %s.%s c ,   %s.%s d
      where
      ed."pat_id"		  =	c."id_1"				              and
      c."field_1"		  =	'%s'		                      and
      c."target"		  =	'%s'		                      and
      ed."icd9_code"	=	d."icd9"				              and
      ed."icd9_code"                                  in
      (select distinct "icd9_code" from %s.%s)        and
      ed."icd9_code"  is not null                     and
      ed."pat_id"                                     in
      (select distinct id_1 from %s.%s c where c."target" = '%s' and c."field_1" = '%s')
      order by "patient_id" asc
      """.format( sourceDbSchemaName(0).get,encounterDiagnosisTableName, etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        etlReferenceDbSchemaName.get,etlReferenceTableName,stagingPatientTableName,field_1
      )

    lazy  val medicalHistoryUpdateQuery       =
      """
       select
       distinct
       c.target_id                                      as "patient_id",
       mh."pat_id"                                      as "pat_id",
       mh."icd9_code"                                   as "icd9_code",
       mh.medical_hx_date                               as "noted_date",
       'Medical History'                                as "source",
       d."id"                                           as "diagnosis_id",
       mh.extract_date                                  as "extract_date",
       mh.md5                                           as "md5"
       FROM
       %s.%s mh,       %s.%s c,  %s.%s d
       where
       mh."pat_id"		  =	c."id_1"				              and
       c."field_1"		  =	'%s'		                      and
       c."target"		  =	'%s'		                        and
       mh."icd9_code"	=	d."icd9"				                and
       mh."icd9_code"                                  in
       (select distinct "icd9_code" from %s.%s)        and
       mh."icd9_code"  is not null                     and
       mh."pat_id"                                     in
       (select distinct id_1 from %s.%s c where c."target" = '%s' and c."field_1" = '%s')
       order by "patient_id" asc
      """.format(  sourceDbSchemaName(0).get,medicalHistoryTableName,etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        etlReferenceDbSchemaName.get,etlReferenceTableName,stagingPatientTableName,field_1
      )


    lazy  val problemListUpdateQuery          =

      """
      select
      distinct
      c.target_id                                     as "patient_id",
      pl."pat_id"                                     as "pat_id",
      pl."icd9_code"                                  as "icd9_code",
      pl.noted_date                                   as "noted_date",
      'Provider Indicated'                            as "source",
      d."id"                                          as "diagnosis_id",
      pl.extract_date                                 as "extract_date",
      pl.md5                                          as "md5"
      FROM
      %s.%s pl    ,   %s.%s c   , %s.%s d
      where
      pl."pat_id"		  =	c."id_1"				              and
      c."field_1"		  =	'%s'		                      and
      c."target"		  =	'%s'		                      and
      pl."icd9_code"	=	d."icd9"				              and
      pl."icd9_code"                                  in
      (select distinct "icd9_code" from %s.%s)        and
      pl."icd9_code"  is not null                     and
      pl."pat_id"                                     in
      (select distinct id_1 from %s.%s c where c."target" = '%s' and c."field_1" = '%s')
      order by "patient_id" asc
      """.format( sourceDbSchemaName(0).get,problemListTableName,etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        etlReferenceDbSchemaName.get,etlReferenceTableName,stagingPatientTableName,field_1
      )



    lazy  val encounterDiagnosisAppendQuery   =
      """
      select
      distinct
      c.target_id                                     as "patient_id",
      ed."pat_id"                                     as "pat_id",
      ed."icd9_code"                                  as "icd9_code",
      ed.contact_date                                 as "noted_date",
      'Billing Data'                                  as "source",
      d."id"                                          as "diagnosis_id",
      ed.extract_date                                 as "extract_date",
      ed.md5                                          as "md5"
      FROM
      %s.%s ed,       %s.%s c ,   %s.%s d
      where
      ed."pat_id"		  =	c."id_1"				              and
      c."field_1"		  =	'%s'		                      and
      c."target"		  =	'%s'		                      and
      ed."icd9_code"	=	d."icd9"				              and
      ed."icd9_code"                                  in
      (select distinct "icd9_code" from %s.%s)        and
      ed."icd9_code"  is not null                     and
      ed."pat_id"                                     not in
      (select distinct id_1 from %s.%s c where c."target" = '%s' and c."field_1" = '%s')
      order by "patient_id" asc
      """.format( sourceDbSchemaName(0).get,encounterDiagnosisTableName, etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        etlReferenceDbSchemaName.get,etlReferenceTableName,stagingPatientTableName,field_1
      )

    lazy  val medicalHistoryAppendQuery       =
      """
       select
       distinct
       c.target_id                                      as "patient_id",
       mh."pat_id"                                      as "pat_id",
       mh."icd9_code"                                   as "icd9_code",
       mh.medical_hx_date                               as "noted_date",
       'Medical History'                                as "source",
       d."id"                                           as "diagnosis_id",
       mh.extract_date                                  as "extract_date",
       mh.md5                                           as "md5"
       FROM
       %s.%s mh,       %s.%s c,  %s.%s d
       where
       mh."pat_id"		  =	c."id_1"				              and
       c."field_1"		  =	'%s'		                      and
       c."target"		  =	'%s'		                        and
       mh."icd9_code"	=	d."icd9"				                and
       mh."icd9_code"                                   in
       (select distinct "icd9_code" from %s.%s)         and
       mh."icd9_code"  is not null                      and
       mh."pat_id"                                      not in
       (select distinct id_1 from %s.%s c where c."target" = '%s' and c."field_1" = '%s')
       order by "patient_id" asc
      """.format(  sourceDbSchemaName(0).get,medicalHistoryTableName,etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        etlReferenceDbSchemaName.get,etlReferenceTableName,stagingPatientTableName,field_1
      )


    lazy  val problemListAppendQuery          =

      """
      select
      distinct
      c.target_id                                     as "patient_id",
      pl."pat_id"                                     as "pat_id",
      pl."icd9_code"                                  as "icd9_code",
      pl.noted_date                                   as "noted_date",
      'Provider Indicated'                            as "source",
      d."id"                                          as "diagnosis_id",
      pl.extract_date                                 as "extract_date",
      pl.md5                                          as "md5"
      FROM
      %s.%s pl    ,   %s.%s c   , %s.%s d
      where
      pl."pat_id"		  =	c."id_1"				              and
      c."field_1"		  =	'%s'		                      and
      c."target"		  =	'%s'		                      and
      pl."icd9_code"	=	d."icd9"				              and
      pl."icd9_code"                                  in
      (select distinct "icd9_code" from %s.%s)        and
      pl."icd9_code"  is not null                     and
      pl."pat_id"                                     not in
      (select distinct id_1 from %s.%s c where c."target" = '%s' and c."field_1" = '%s')
      order by "patient_id" asc
      """.format( sourceDbSchemaName(0).get,problemListTableName,etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        etlReferenceDbSchemaName.get,etlReferenceTableName,stagingPatientTableName,field_1
      )


    // pat_enc_csn_id  based


    lazy  val encounterDiagnosisIdsQuery    =
      """
      select
      distinct
      c.target_id                                     as "patient_id",
      ed."pat_id"                                     as "pat_id",
      ed."icd9_code"                                  as "icd9_code",
      ed.contact_date                                 as "noted_date",
      'Billing Data'                                  as "source",
      d."id"                                          as "diagnosis_id",
      ed.extract_date                                 as "extract_date",
      ed.md5                                          as "md5"
      FROM
      %s.%s ed,       %s.%s c ,   %s.%s d
      where
      ed."pat_id"		  =	c."id_1"				              and
      c."field_1"		  =	'%s'		                      and
      c."target"		  =	'%s'		                      and
      ed."icd9_code"	=	d."icd9"				              and
      ed."icd9_code"                                  in
      (select distinct "icd9_code" from %s.%s)        and
      ed."icd9_code"  is not null                     and
      ed."pat_id"     = '%s'
      order by "patient_id" asc
      """.format( sourceDbSchemaName(0).get,encounterDiagnosisTableName, etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        pat_enc_csn_id
      )

    lazy  val medicalHistoryIdsQuery       =
      """
       select
       distinct
       c.target_id                                      as "patient_id",
       mh."pat_id"                                      as "pat_id",
       mh."icd9_code"                                   as "icd9_code",
       mh.medical_hx_date                               as "noted_date",
       'Medical History'                                as "source",
       d."id"                                           as "diagnosis_id",
       mh.extract_date                                  as "extract_date",
       mh.md5                                           as "md5"
       FROM
       %s.%s mh,       %s.%s c,  %s.%s d
       where
       mh."pat_id"		  =	c."id_1"				              and
       c."field_1"		  =	'%s'		                      and
       c."target"		  =	'%s'		                        and
       mh."icd9_code"	=	d."icd9"				                and
       mh."icd9_code"                                   in
       (select distinct "icd9_code" from %s.%s)         and
       mh."icd9_code"  is not null                      and
       mh."pat_id"  = '%s'
       order by "patient_id" asc
      """.format(  sourceDbSchemaName(0).get,medicalHistoryTableName,etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        etlReferenceDbSchemaName.get,etlReferenceTableName,stagingPatientTableName,field_1,
        pat_enc_csn_id
      )


    lazy  val problemListIdsQuery          =

      """
      select
      distinct
      c.target_id                                     as "patient_id",
      pl."pat_id"                                     as "pat_id",
      pl."icd9_code"                                  as "icd9_code",
      pl.noted_date                                   as "noted_date",
      'Provider Indicated'                            as "source",
      d."id"                                          as "diagnosis_id",
      pl.extract_date                                 as "extract_date",
      pl.md5                                          as "md5"
      FROM
      %s.%s pl    ,   %s.%s c   , %s.%s d
      where
      pl."pat_id"		  =	c."id_1"				              and
      c."field_1"		  =	'%s'		                      and
      c."target"		  =	'%s'		                      and
      pl."icd9_code"	=	d."icd9"				              and
      pl."icd9_code"                                  in
      (select distinct "icd9_code" from %s.%s)        and
      pl."icd9_code"  is not null                     and
      pl."pat_id"     = '%s'
      order by "patient_id" asc
      """.format( sourceDbSchemaName(0).get,problemListTableName,etlReferenceDbSchemaName.get,etlReferenceTableName,
        targetDbSchemaName(0).get, stagingDiagnosisTableName,
        field_1,stagingPatientTableName,sourceDbSchemaName(0).get,icd9AllowedTableName,
        etlReferenceDbSchemaName.get,etlReferenceTableName,stagingPatientTableName,field_1,
        pat_enc_csn_id
      )





    if (mode == "update")      {        //Existing Patients

      if (ids.isEmpty)  {

        if (element.get == "encounter_diagnosis")    {

           encounterDiagnosisUpdateQuery

        }
        else if (element.get == "medical_history")    {

          medicalHistoryUpdateQuery

        }
        else if (element.get == "problemlist")    {

          problemListUpdateQuery

        }
        else  {

          println("Bad Operation.  Bad Update Element.  Element Query Name Must be encounter_diagnosis, medical_history, or problemlist")

          sys.exit()
        }

      }
      else          {

        if (element.get == "encounter_diagnosis")    {

          encounterDiagnosisIdsQuery

        }
        else if (element.get == "medical_history")    {

          medicalHistoryIdsQuery

        }
        else if (element.get == "problemlist")    {

          problemListIdsQuery

        }
        else  {

          println("Bad Operation.  Bad Update Element.  Element Query Name Must be encounter_diagnosis, medical_history, or problemlist")

          sys.exit()
        }

      }

    }
    else  if (mode == "append")      {        //New Patients

      if (ids.isEmpty)  {

        if (element.get == "encounter_diagnosis")    {

          encounterDiagnosisAppendQuery

        }
        else if (element.get == "medical_history")    {

          medicalHistoryAppendQuery

        }
        else if (element.get == "problemlist")    {

          problemListAppendQuery

        }
        else  {

          println("Bad Operation.  Bad Update Element.  Element Query Name Must be encounter_diagnosis, medical_history, or problemlist")

          sys.exit()
        }

      }
      else          {

        if (element.get == "encounter_diagnosis")    {

          encounterDiagnosisIdsQuery

        }
        else if (element.get == "medical_history")    {

          medicalHistoryIdsQuery

        }
        else if (element.get == "problemlist")    {

          problemListIdsQuery

        }
        else  {

          println("Bad Operation.  Bad Update Element.  Element Query Name Must be encounter_diagnosis, medical_history, or problemlist")

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


object DiagnosisTransform  {

  var defaultSourceTableNames: Seq[String]      = Seq("problemlist","encounter_diagnosis","medical_history","staging_diagnosis")

  var defaultTargetTableNames: Seq[String]      = Seq("staging_diagnosisdetail")

  var defaultSourceSchemaName                   = "qe11b"

  var defaultTargetSchemaName                   = "qe11c"

  val defaultSourcePropertiesFP                 =
    "conf/connection-properties/transform-source.properties"

  val defaultTargetPropertiesFP                 =
    "conf/connection-properties/transform-target.properties"


  def apply(): DiagnosisTransform   =   {

    try {

      new DiagnosisTransform  (
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



  def apply(query: String): DiagnosisTransform  =   {

    try {

      new DiagnosisTransform  (
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

