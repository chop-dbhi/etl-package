package edu.chop.cbmi.etl.audgendb.transform

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

import java.util.Calendar

import scala.util.control.Breaks.breakable
import scala.util.control.Breaks.break

import scala.util.matching.Regex


case class AudiogramTransform(  override val sdbfp: String,
                                override val tdbfp: String,
                                override val sourceTableNames:    Seq[String],
                                override val targetTableNames:    Seq[String],
                                override val sourceDbSchemaName:  Seq[Option[String]],
                                override val targetDbSchemaName:  Seq[Option[String]],
                                override val query: Option[String])
  extends AudgendbSqlTransform(sdbfp,tdbfp, sourceTableNames, targetTableNames, sourceDbSchemaName, targetDbSchemaName, query) {


  val audiogramConceptId                    =   "CHOP#AUDIO#0001"

  val audiogramConceptValueHeader           =   "Ear;Stim;125;250;500;750;1000;1500;2000;3000;4000;6000;8000"

  val frequencyFlagRegexPattern             =   """(\d{1,3})(\w{0,3})"""
  
  val frequencyFlagRegex                    =   new Regex(frequencyFlagRegexPattern,"frequency","flags")

  val valueRegexPattern                     =   "^" + audiogramConceptValueHeader + "(.*)"

  //  Only have one sub-group, so the value of the subgroup is the same as the value of all the subgroups combined
  //  Python: raw_lines = p.groups()[0].split(';')
  val valueRegex                            =   new Regex(valueRegexPattern,"audiogramValue")

  val frequency:Seq[String]                 =
    Seq("f125","f250","f500","f750","f1000","f1500","f2000","f3000","f4000","f6000","f8000")

  val frequencyFlag:Seq[String]             =
    Seq (
        "f125_flags","f250_flags","f500_flags","f750_flags","f1000_flags","f1500_flags",
        "f2000_flags","f3000_flags","f4000_flags","f6000_flags","f8000_flags"
        )
  
  val frequencyFlagMap                      =     frequency.zip(frequencyFlag).toMap

  var earsNormal:Map[String, Boolean]       =     Map.empty

  var audiogramEncounterResultData: 
  MutableList[
    (   Map[String, String],          //  Ear and Stimulus
        Map[String, Int],             //  Frequency
        Map[String, String],          //  Flags
        Map[String, Boolean],         //  earIsNormal
        Map[String, Double],          //  pta
        Map[String, Boolean],         //  ptaWorse
        Map[String, Double],          //  pta4
        Map[String, Boolean],         //  pta4Worse
        Map[String, Boolean],         //  Is Worse Severity
        Map[String, String],          //  Shape
        Map[String, String],          //  Severity
        Map[String, Int]              //  audiogramResultId
    )
  ]
  = MutableList.empty

  
  var audiogramEncounterTestData: 
    (
      Map[String,String],         //  lossSymmetry
      Map[String,Boolean],        //  hasConductiveLoss
      Map[String,Boolean],        //  hasSensorineuralLoss
      Map[String,String],         //  betterEarStatus
      Map[String,Int],            //  betterEarId
      Map[String,Int],            //  worseEarId
      Map[String,Int]             //  audiogramTestId
    ) = (Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty)
     
  val currentPatientIdEncounterId: Map[String, Int] =     Map ("patientId"  ->  0,  "encounterId" ->  0)

  var audiogramEarStimulus: Map[String, String]     =     Map ("ear" -> null, "stimulus" -> null)

  var frequencyResults: Map[String, Int]            =     Map.empty

  var frequencyResultsFlags: Map[String, String]    =     Map.empty

  val lossSymmetry            =     Map  (
                                          "Normal"            ->  "Normal",
                                          "Unilateral"        ->  "Unilateral",
                                          "Bilateral"         ->  "Bilateral",
                                          "Insufficient Data" ->  "Insufficient Data",
                                          "None"              ->  "Not Calculated"
                                    ) 
  
  val betterEarStatusChoices  =     Map  (
                                          "Right"             -> "Right",
                                          "Left"              -> "Left",
                                          "Same"              -> "Same",
                                          "Insufficient Data" -> "Insufficient Data",
                                          "None"              -> "Not Calculated"
                                    )


  val severityChoices         =     Map (
                                          "Normal"              -> "Normal",
                                          "Slight"              -> "Slight",
                                          "Mild"                -> "Mild",
                                          "Moderate"            -> "Moderate",
                                          "Moderately Severe"   -> "Moderately Severe",
                                          "Severe"              -> "Severe",
                                          "Profound"            -> "Profound",
                                          "Insufficient Data"   -> "Insufficient Data"
                                    )

  val shapeChoices            =     Map (
                                          "Has NRs"             -> "Has NRs",
                                          "All <30"             -> "All <30",
                                          "Sloping"             -> "Sloping",
                                          "Rising"              -> "Rising",
                                          "Flat"                -> "Flat",
                                          "U-Shaped"            -> "U-Shaped",
                                          "Tent-Shaped"         -> "Tent-Shaped",
                                          "Other"               -> "Other",
                                          "Insufficient Data"   -> "Insufficient Data"
                                    )

  processBackend.connect()

  targetBackend.connection.setAutoCommit(false)


  override def update:  Boolean  =  {

    try {

      generateSourceResultSet(None,None)

      //generateSourceResultSetPerEncounter(11678360)

      resourceId(resourceName)        =   retrieveResourceId(resourceName)

      while (sourceResult.next  && clear)     {

        generateEncounterAudiogramResultData(sourceResult.getString("concept_value"))

        if (retrieveEncounterIdPatientIdByPECId(sourceResult.getString("pat_enc_csn_id")) )   {

          val patientId                                     =   currentPatientIdEncounterId("patientId")

          val encounterId                                   =   currentPatientIdEncounterId("encounterId")

          generateEncounterAudiogramTestInitialData

          audiogramEncounterTestData._7("audiogramTestId")  =   insertAudiogramTestRecord(patientId,encounterId)

          for (resultIndex <- 0 to audiogramEncounterResultData.length - 1)   {

            /*  Insert one audiogram result record for each audiogramEncounterResultData */
            audiogramEncounterResultData(resultIndex)._12("audiogramResultId")  =
              insertAudiogramResultRecord(resultIndex)

          }

          generateEncounterAudiogramTestRemainingData

          updateAudiogramTestRecord

          //Only Commit if All Inserts and Updates are successful
          targetBackend.commit()

        }

      }

      close

      true
    }
    catch {
      case e:RuntimeException        => println("Transform Failed:  Closing Backend Connections")

      close

      throw e
    }

  }

  override def generateSourceResultSet(element:Option[String]  = None, ids:Option[Seq[Int]] = None):
  Boolean  =   {

    if (ids.isEmpty)  {
      
    
      sourceQuery         =
        """
        SELECT  DISTINCT  * FROM %s.%s
        WHERE
        concept_id = '%s'   AND
        concept_value       LIKE  '%s%s'
        ORDER BY            pat_enc_csn_id  ASC
        """.format (sourceDbSchemaName(0).get,sourceTableNames(0),audiogramConceptId,audiogramConceptValueHeader,"%")
  
      try {
  
        sourceResult     =   sourceBackend.executeQuery(sourceQuery,DataRow.empty)
  
      }
      catch {
        case e:java.sql.SQLException  =>
  
          close
  
          println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")
  
          throw e
      }
    
    }
    else  {

      val  pat_enc_csn_id = ids.get.seq(0)

      sourceQuery         =
        """
        SELECT   DISTINCT * FROM %s.%s
        WHERE
        concept_id = '%s'                         AND
        concept_value LIKE '%s%s'                 AND
        pat_enc_csn_id = '%s'
        """.format (sourceDbSchemaName(0).get,sourceTableNames(0),audiogramConceptId,audiogramConceptValueHeader,"%s",pat_enc_csn_id)


      try {

        sourceResult     =   sourceBackend.executeQuery(sourceQuery,DataRow.empty)

      }
      catch {
        case e:java.sql.SQLException  =>

          close

          println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

          throw e

      }
      
    }

    true

  }

  def generateEncounterAudiogramResultData(conceptAudiogramData: String) : Boolean = {

    var audiogramDataSets:  MutableList[List[String]]     =   new MutableList()

    var set: List[String]                                 =   List.empty

    val setWidth                                          =   13

    var start:Int = 0

    var end:Int   =  setWidth

    val audiogramValueList    =   valueRegex.findAllIn(conceptAudiogramData).matchData.toList

    val audiogramValue        =   audiogramValueList(0).group("audiogramValue")

    val lines:List[String]                                =   audiogramValue.split(";").toList.map(x => x.trim)
    //Ceil:  Some sets seems to be shortened
    val numberOfSets:Float                                =   (lines.length.toFloat/setWidth).ceil

    for (i <- 1 to numberOfSets.toInt){

      set = lines.slice(start,end)
      audiogramDataSets   +=  set
      start               =   start + setWidth
      end                 =   end   + setWidth

    }

    for (resultSet <- audiogramDataSets)  {

      audiogramEncounterResultData    +=
        ((Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty))

      val resultIndex:Int =   audiogramEncounterResultData.length - 1

      //  Set Ear
      audiogramEncounterResultData(resultIndex)._1("ear")                 =   
       resultSet(0).toLowerCase   match           {
        case  "l"       =>  "Left"
        case  "r"       =>  "Right"
        case  "s"       =>  "Sound Field"
        case  "b"       =>  "Both"
        case  _         =>  resultSet(0)
       }  

      //  Set Stimulus    - Capitalize the first letter
      audiogramEncounterResultData(resultIndex)._1("stimulus")            =
       resultSet(1).toLowerCase   match           {
        case  "a"       =>  "Air"
        case  "air"     =>  "Air"
        case  "aid"     =>  "Aided"
        case  "bone"    =>  "Bone"
        case  "c"       =>  "Cochlear Implant"
        case  "ci"      =>  "Cochlear Implant"
        case  "w"       =>  "Warble"
        case  _         =>  resultSet(1)(0).toUpper + resultSet(1).substring(1,resultSet(1).length).toLowerCase
       }



      for (i <- 2 to  (resultSet.length - 1)){

        val frequencyFlagMatchList  =   frequencyFlagRegex.findAllIn(resultSet(i)).matchData.toList

        if  (frequencyFlagMatchList.indices.length > 0 )      {

          //Skip over empties
          if (!(frequencyFlagMatchList(0).group("frequency").isEmpty)) {

            //f125 is a frequency(0) and so on
            audiogramEncounterResultData(resultIndex)._2(frequency(i - 2))              =       
              frequencyFlagMatchList(0).group("frequency").toInt

          }

          if (!(frequencyFlagMatchList(0).group("flags").isEmpty))     {

            audiogramEncounterResultData(resultIndex)._3(frequencyFlag(i - 2))          =
              frequencyFlagMatchList(0).group("flags")

          }

        }

      }


      if  (
            (audiogramEncounterResultData(resultIndex)._1.getOrElse("stimulus",null).toLowerCase   ==    "air")   &&
            (audiogramEncounterResultData(resultIndex)._2.getOrElse("f500",null)       !=    null)                &&
            (audiogramEncounterResultData(resultIndex)._2.getOrElse("f1000",null)      !=    null)                &&
            (audiogramEncounterResultData(resultIndex)._2.getOrElse("f2000",null)      !=    null)
          )   {

        val ptaFrequencies: Seq[Int]  =   Seq(
                                              audiogramEncounterResultData(resultIndex)._2("f500"),
                                              audiogramEncounterResultData(resultIndex)._2("f1000"),
                                              audiogramEncounterResultData(resultIndex)._2("f2000")
                                          )

        audiogramEncounterResultData(resultIndex)._5("pta")               =         calculatePTA(ptaFrequencies)

        val  ptaFlags: MutableList[String]        =   MutableList.empty

        audiogramEncounterResultData(resultIndex)._3.foreach  {

          case   (  frequencyFlagName, frequencyFlagValue )   =>

            if  (
                  (frequencyFlagName == "f500_flags") || (frequencyFlagName == "f1000_flags") ||
                  (frequencyFlagName == "f2000_flags" )
                )

            ptaFlags  +=  frequencyFlagValue

        }

        audiogramEncounterResultData(resultIndex)._6("ptaWorse")          =         calculatePTAWorse(ptaFlags)

      }

      if  (
          (audiogramEncounterResultData(resultIndex)._1.getOrElse("stimulus",null).toLowerCase   ==    "air")   &&
          (audiogramEncounterResultData(resultIndex)._2.getOrElse("f500",null)       !=    null)                &&
          (audiogramEncounterResultData(resultIndex)._2.getOrElse("f1000",null)      !=    null)                &&
          (audiogramEncounterResultData(resultIndex)._2.getOrElse("f2000",null)      !=    null)                &&
          (audiogramEncounterResultData(resultIndex)._2.getOrElse("f4000",null)      !=    null)
      )   {

        val pta4Frequencies: Seq[Int]  =     Seq(
                                                  audiogramEncounterResultData(resultIndex)._2("f500"),
                                                  audiogramEncounterResultData(resultIndex)._2("f1000"),
                                                  audiogramEncounterResultData(resultIndex)._2("f2000"),
                                                  audiogramEncounterResultData(resultIndex)._2("f4000")
                                              )

        audiogramEncounterResultData(resultIndex)._7("pta4")              =         calculatePTA4(pta4Frequencies)



        val  pta4Flags: MutableList[String]        =   MutableList.empty

        audiogramEncounterResultData(resultIndex)._3.foreach  {

          case   (  frequencyFlagName, frequencyFlagValue )   =>

            if  (
                  (frequencyFlagName == "f500_flags")     ||    (frequencyFlagName == "f1000_flags")  ||
                  (frequencyFlagName == "f2000_flags")    ||    (frequencyFlagName == "f4000_flags")
                )

              pta4Flags  +=  frequencyFlagValue

        }


        audiogramEncounterResultData(resultIndex)._8("pta4Worse")         =         calculatePTA4Worse(pta4Flags)
      }


      if (audiogramEncounterResultData(resultIndex)._5.getOrElse("pta",null)  !=  null)  {
        //If pta is calculated than ptaWorse would be calculated
        val severities: (Boolean, String)    =
          calculateSeverities(  audiogramEncounterResultData(resultIndex)._5("pta"),
                                audiogramEncounterResultData(resultIndex)._6("ptaWorse")
          )

        audiogramEncounterResultData(resultIndex)._9("isWorseSeverity")   = severities._1
        audiogramEncounterResultData(resultIndex)._11("severity")         = severities._2
      }


      if  ( (audiogramEncounterResultData(resultIndex)._1.getOrElse("stimulus",null).toLowerCase   ==    "air") )     {
        
        if  (
              (audiogramEncounterResultData(resultIndex)._2.getOrElse("f250",null)    !=    null)     &&
              (audiogramEncounterResultData(resultIndex)._2.getOrElse("f500",null)    !=    null)     &&
              (audiogramEncounterResultData(resultIndex)._2.getOrElse("f1000",null)   !=    null)     &&
              (audiogramEncounterResultData(resultIndex)._2.getOrElse("f2000",null)   !=    null)     &&
              (audiogramEncounterResultData(resultIndex)._2.getOrElse("f4000",null)   !=    null)     &&
              (audiogramEncounterResultData(resultIndex)._2.getOrElse("f8000",null)   !=    null)
            ) {


          val  shapesFlags: MutableList[String]        =   MutableList.empty


          audiogramEncounterResultData(resultIndex)._3.foreach  {

            case   (  frequencyFlagName, frequencyFlagValue )   =>

              if  (
                    (frequencyFlagName == "f250_flags")     ||  (frequencyFlagName == "f500_flags")     ||
                    (frequencyFlagName == "f1000_flags")    ||  (frequencyFlagName == "f2000_flags")    ||
                    (frequencyFlagName == "f4000_flags")    ||  (frequencyFlagName == "f8000_flags")
                  )

                shapesFlags  +=  frequencyFlagValue

          }


          audiogramEncounterResultData(resultIndex)._10("shape")  =
            calculateShapes(MutableList (
                                audiogramEncounterResultData(resultIndex)._2("f250"),
                                audiogramEncounterResultData(resultIndex)._2("f500"),
                                audiogramEncounterResultData(resultIndex)._2("f1000"),
                                audiogramEncounterResultData(resultIndex)._2("f2000"),
                                audiogramEncounterResultData(resultIndex)._2("f4000"),
                                audiogramEncounterResultData(resultIndex)._2("f8000")
                            ),
                            shapesFlags
            )
        }
        else  {
          //Insufficient Data
          audiogramEncounterResultData(resultIndex)._10("shape")   = shapeChoices("Insufficient Data")
        }

      }
      
      
      
      
      

    }

    //After the majority of the audiogram test level results have been parsed and calculated ...


    var hasEarNormalLeftAirResults:  Boolean           =  false
    
    var hasEarsNormalRightAirResults:  Boolean         =  false

    val earsNormalFrequencies = Seq("f500","f1000","f2000","f4000")

    for (result <-  audiogramEncounterResultData) {

      if  (
                (result._1.getOrElse("ear",null).toLowerCase ==  "left" )                   &&
                (result._1.getOrElse("stimulus",null).toLowerCase  ==  "air" )              &&
                (
                  ( //Make sure all the earsNormalFrequencies have results in left ear
                    (result._2.filterKeys(earsNormalFrequencies.contains).toList.length) ==
                    earsNormalFrequencies.length
                  )
                )
          )   {

        hasEarNormalLeftAirResults  = true

      }
      else  if  (
                    (result._1.getOrElse("ear",null).toLowerCase ==  "right" )              &&
                    (result._1.getOrElse("stimulus",null).toLowerCase  ==  "air" )          &&
                    (
                      ( //Make sure all the earsNormalFrequencies have results in right ear
                        (result._2.filterKeys(earsNormalFrequencies.contains).toList.length) ==
                        earsNormalFrequencies.length
                      )
                    )
                )   {

        hasEarsNormalRightAirResults  = true

      }
      

    }

    if (hasEarNormalLeftAirResults  && hasEarsNormalRightAirResults)  {  doEarsNormal }

    true
  }

  def generateEncounterAudiogramTestInitialData: Unit  = {

    doLossSymmetry

    doTypeOfLoss

  }

  def generateEncounterAudiogramTestRemainingData: Unit  = {

    doBetterEarWorseEar

  }

  def insertAudiogramResultRecord(resultIndex:Int): Int     =     {

    val createdDate                           =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
    
    val modifiedDate                          =   createdDate

    var targetAudiogramDataRow: DataRow[Any]  =   DataRow.empty

    var etlReferenceRow: DataRow[Any]         =   DataRow.empty

    var audiogramResultId:Int                 =   0

    val lastInsertedAudiogramResultIdQuery: String    =
        "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get,targetTableNames(1))

    targetAudiogramDataRow    =
      DataRow(
                ("created",             createdDate),
                ("modified",            modifiedDate),
                ("ear",                 audiogramEncounterResultData(resultIndex)._1.getOrElse("ear",null)),
                ("stimulus",            audiogramEncounterResultData(resultIndex)._1.getOrElse("stimulus",null)),
                ("f125",                audiogramEncounterResultData(resultIndex)._2.getOrElse("f125",null)),
                ("f125_flags",          audiogramEncounterResultData(resultIndex)._3.getOrElse("f125_flags",null)),
                ("f250",                audiogramEncounterResultData(resultIndex)._2.getOrElse("f250",null)),
                ("f250_flags",          audiogramEncounterResultData(resultIndex)._3.getOrElse("f250_flags",null)),
                ("f500",                audiogramEncounterResultData(resultIndex)._2.getOrElse("f500",null)),
                ("f500_flags",          audiogramEncounterResultData(resultIndex)._3.getOrElse("f500_flags",null)),
                ("f750",                audiogramEncounterResultData(resultIndex)._2.getOrElse("f750",null)),
                ("f750_flags",          audiogramEncounterResultData(resultIndex)._3.getOrElse("f750_flags",null)),
                ("f1000",               audiogramEncounterResultData(resultIndex)._2.getOrElse("f1000",null)),
                ("f1000_flags",         audiogramEncounterResultData(resultIndex)._3.getOrElse("f1000_flags",null)),
                ("f1500",               audiogramEncounterResultData(resultIndex)._2.getOrElse("f1500",null)),
                ("f1500_flags",         audiogramEncounterResultData(resultIndex)._3.getOrElse("f1500_flags",null)),
                ("f2000",               audiogramEncounterResultData(resultIndex)._2.getOrElse("f2000",null)),
                ("f2000_flags",         audiogramEncounterResultData(resultIndex)._3.getOrElse("f2000_flags",null)),
                ("f3000",               audiogramEncounterResultData(resultIndex)._2.getOrElse("f3000",null)),
                ("f3000_flags",         audiogramEncounterResultData(resultIndex)._3.getOrElse("f3000_flags",null)),
                ("f4000",               audiogramEncounterResultData(resultIndex)._2.getOrElse("f4000",null)),
                ("f4000_flags",         audiogramEncounterResultData(resultIndex)._3.getOrElse("f4000_flags",null)),
                ("f6000",               audiogramEncounterResultData(resultIndex)._2.getOrElse("f6000",null)),
                ("f6000_flags",         audiogramEncounterResultData(resultIndex)._3.getOrElse("f6000_flags",null)),
                ("f8000",               audiogramEncounterResultData(resultIndex)._2.getOrElse("f8000",null)),
                ("f8000_flags",         audiogramEncounterResultData(resultIndex)._3.getOrElse("f8000_flags",null)),
                ("is_normal",           audiogramEncounterResultData(resultIndex)._4.getOrElse("earIsNormal",null)),
                (
                  "pta",
                  if (audiogramEncounterResultData(resultIndex)._5.getOrElse ("pta",null) !=  null)   {
                    audiogramEncounterResultData(resultIndex)._5("pta")
                  }
                  else  { null  }

                ),
                ("pta_worse",           audiogramEncounterResultData(resultIndex)._6.getOrElse("ptaWorse",null)),
                (
                  "pta4",
                  if  (audiogramEncounterResultData(resultIndex)._7.getOrElse("pta4",null)  != null)  {
                    audiogramEncounterResultData(resultIndex)._7("pta4")
                  }
                  else  { null  }
                ),
                ("pta4_worse",          audiogramEncounterResultData(resultIndex)._8.getOrElse("pta4Worse",null)),
                ("is_worse_severity",   audiogramEncounterResultData(resultIndex)._9.getOrElse("isWorseSeverity",null)),
                ("shape",               audiogramEncounterResultData(resultIndex)._10.getOrElse("shape",null)),
                ("severity",            audiogramEncounterResultData(resultIndex)._11.getOrElse("severity",null)),
                ("test_id",             audiogramEncounterTestData._7("audiogramTestId"))

      )

    //Insert into staging_audiogramresult
    targetBackend.insertRow(targetTableNames(1),targetAudiogramDataRow,targetDbSchemaName(0))

    try   {
      val lastInsertedAudiogramResultIdResult           =   targetBackend.executeQuery(lastInsertedAudiogramResultIdQuery)
      lastInsertedAudiogramResultIdResult.next()
      audiogramResultId                                 =   lastInsertedAudiogramResultIdResult.getInt("id")
    }
    catch {
      case e:RuntimeException        => println("last inserted audiogram result id could not be retrieved")
      throw e
    }

    etlReferenceRow                       =   DataRow(  ("resource_id",resourceId(resourceName)),
                                                        ("field_1","concept.pat_enc_csn_id"),
                                                        ("id_1",sourceResult.getString("pat_enc_csn_id")),
                                                        ("target","staging_audiogramresult"),
                                                        ("target_id",audiogramResultId)
                                              )

    //Insert into staging data source
    targetBackend.insertRow(etlReferenceTableName,etlReferenceRow,etlReferenceDbSchemaName)

    audiogramResultId
  }

  def insertAudiogramTestRecord(patientId:Int,encounterId:Int):Int   =  {

    /*  Insert for Audiogram Test   */
    /*  Insert one audiogram test record for each Encounter   */

    val createdDate                           =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

    val modifiedDate                          =   createdDate

    var targetAudiogramTestRow: DataRow[Any]        =   DataRow.empty

    var etlReferenceRow: DataRow[Any]               =   DataRow.empty

    var audiogramTestId                             =   0

    val lastInsertedAudiogramTestIdQuery: String    =
        "SELECT currval(pg_get_serial_sequence('%s.%s','id')) AS id".format(targetDbSchemaName(0).get,targetTableNames(0))

    targetAudiogramTestRow                =
      DataRow(      ("created",createdDate) ,
                    ("modified",modifiedDate),
                    ("patient_id",patientId),
                    ("encounter_id",encounterId),
                    ("loss_symmetry",audiogramEncounterTestData._1("lossSymmetry")  ),
                    (
                      "has_conductive_loss",
                      if  (audiogramEncounterTestData._2.getOrElse("hasConductiveLoss",null)  !=  null) {
                        audiogramEncounterTestData._2("hasConductiveLoss")
                      }
                      else {  null  }
                    ),
                    (
                      "has_sensorineural_loss",
                      if  (audiogramEncounterTestData._3.getOrElse("hasSensorineuralLoss",null) !=  null) {
                        audiogramEncounterTestData._3("hasSensorineuralLoss")
                      }
                      else  { null  }
                    ),    //has not been calculated yet but can not be null
                    ("better_ear_status",betterEarStatusChoices("Insufficient Data")  )
      )

    //Insert into staging_audiogramtest
    targetBackend.insertRow(targetTableNames(0),targetAudiogramTestRow,targetDbSchemaName(0))

    try   {
      val lastInsertedAudiogramTestIdResult             =   targetBackend.executeQuery(lastInsertedAudiogramTestIdQuery)
      lastInsertedAudiogramTestIdResult.next()
      audiogramTestId                                   =   lastInsertedAudiogramTestIdResult.getInt("id")
    }
    catch {
      case e:RuntimeException        => println("last inserted auiogram test id could not be retrieved")
      throw e
    }

    etlReferenceRow                       =   DataRow(      ("resource_id",resourceId(resourceName)),
                                                            ("field_1","concept.pat_enc_csn_id"),
                                                            ("id_1",sourceResult.getString("pat_enc_csn_id")),
                                                            ("target","staging_audiogramtest"),
                                                            ("target_id",audiogramTestId)

                                              )

    //Insert into staging data source
    targetBackend.insertRow(etlReferenceTableName,etlReferenceRow,etlReferenceDbSchemaName)

    audiogramTestId
  }

  def updateAudiogramTestRecord:  Boolean   = {

    val modifiedDate                          =   new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

    var targetAudiogramTestRow: DataRow[Any]  =   DataRow.empty

    targetAudiogramTestRow                    =
      DataRow(
                    ("modified",modifiedDate),
                    ("loss_symmetry",audiogramEncounterTestData._1("lossSymmetry")),
                    (
                      "has_conductive_loss",
                      if  (audiogramEncounterTestData._2.getOrElse("hasConductiveLoss",null)  !=  null) {
                        audiogramEncounterTestData._2("hasConductiveLoss")
                      }
                      else {  null  }
                    ),
                    (
                      "has_sensorineural_loss",
                      if  (audiogramEncounterTestData._3.getOrElse("hasSensorineuralLoss",null) !=  null) {
                        audiogramEncounterTestData._3("hasSensorineuralLoss")
                      }
                      else  { null  }
                    ),
                    ("better_ear_status",audiogramEncounterTestData._4("betterEarStatus")),
                    (
                      "better_ear_id",
                      if  (audiogramEncounterTestData._5.getOrElse("betterEarId",null)  !=  null) {
                        audiogramEncounterTestData._5("betterEarId")
                      }
                      else  { null  }
                    ),
                    (
                      "worse_ear_id",
                      if  (audiogramEncounterTestData._6.getOrElse("worseEarId",null)   !=  null) {
                        audiogramEncounterTestData._6("worseEarId")
                      }
                      else  { null  }
                    )
      )


    targetBackend.updateRow(targetTableNames(0),targetAudiogramTestRow,List(("id",audiogramEncounterTestData._7("audiogramTestId"))),targetDbSchemaName(0))

    true
  }

  def retrieveEncounterIdPatientIdByPECId(pat_enc_csn_id: String):  Boolean     =     {
    //Get the staged encounter id and patient id by data sourced pat_enc_csn_id
    val dbSchemaName          =     targetDbSchemaName(0).get  // In General the Target is staging database for a transform

    val audiologySpecialty    =     "Audiology"

    val target                =     "staging_encounter"

    val fields                =
      ("encounter.pat_enc_csn_id","mriencounter.pat_enc_csn_id","ctencounter.pat_enc_csn_id","hospitalencounter.pat_enc_csn_id")

    sourceQuery       =   """
                          SELECT DISTINCT e.id, e.patient_id
                          FROM
                          %s.%s e, %s.%s cd
                          WHERE
                          cd.target_id  =   e.id      AND
                          cd.id_1 = '%s'              AND
                          cd.field_1 in  ('%s','%s','%s','%s')  AND
                          cd.target = '%s'
                          """.format (dbSchemaName,sourceTableNames(1),dbSchemaName,sourceTableNames(2),pat_enc_csn_id,
                                      fields._1,fields._2,fields._3,fields._4,target
                          )


    try {

      val queryResult     =   processBackend.executeQuery(sourceQuery,DataRow.empty)
      if (!queryResult.next){

        log.write("Data Sourced patient id and encounter id was not found for encounter " + pat_enc_csn_id)

        return  false
        //sys.exit()
      }

          currentPatientIdEncounterId("encounterId")        =    queryResult.getInt("id")
          currentPatientIdEncounterId("patientId")          =    queryResult.getInt("patient_id")


      return  true
    }
    catch {
      case e:java.sql.SQLException  =>
        close
        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")
        throw e
    }

    close
    return false

  }

  def calculatePTA(values:  Seq[Int]):  Double    = {
    
    //val scale = 13

    //val x = BigDecimal(values.sum/3.0,  scale)

    //x

    values.sum/3.0

  }

  def calculatePTAWorse(values:  MutableList[String]):Boolean  = {

      values.foreach(i => if ( i.contains('N')  )  {  return true   } )

      return  false
  }

  def calculatePTA4(values:  Seq[Int]): Double    = {

    //val scale = 13

    //val x = BigDecimal(values.sum/4.0,scale)

    //x

    values.sum/4.0

  }

  def calculatePTA4Worse(values:  MutableList[String]):Boolean  = {

    values.foreach(i => if ( i.contains('N')  )  {  return true   }  )

    return  false
  }

  def doEarsNormal  {

    var hasLeftEarResults:Boolean     =   false

    var hasRightEarResults:Boolean    =   false

    /*  Check to see if we have both left an right ear results  */
    for   (result <- audiogramEncounterResultData)  {

      if (hasLeftEarResults   ==  false)  {

        hasLeftEarResults   =
            ( (result._1.getOrElse("ear",null).toLowerCase      ==  "left")  &&
              (result._1.getOrElse("stimulus",null).toLowerCase ==  "air")   &&
              (result._2.getOrElse("f500",null)    !=  null)                 &&
              (result._2.getOrElse("f1000",null)   !=  null)                 &&
              (result._2.getOrElse("f2000",null)   !=  null)                 &&
              (result._2.getOrElse("f4000",null)   !=  null)
            )
      }

      
      if (hasRightEarResults  ==  false)  {

        hasRightEarResults  =
          ( (result._1.getOrElse("ear",null).toLowerCase ==  "right")       &&
            (result._1.getOrElse("stimulus",null).toLowerCase ==  "air")    &&
            (result._2.getOrElse("f500",null)    !=  null)                  &&
            (result._2.getOrElse("f1000",null)   !=  null)                  &&
            (result._2.getOrElse("f2000",null)   !=  null)                  &&
            (result._2.getOrElse("f4000",null)   !=  null)
          )
      }


    }

    /*  If we have both left and right ear results, set earIsNormal to true or false for each ear */
    if (hasLeftEarResults && hasRightEarResults) {

      earsNormal =     Map( "leftEarIsNormal"       -> false,
                            "leftEarIsCalculated"   -> false,
                            "rightEarIsNormal"      -> false,
                            "rightEarIsCalculated"  -> false
                       )
      
      

      for   (result <- audiogramEncounterResultData)  {

        val earsNormalFrequencies: Map[String, Int]   =   Map.empty

        //Get all the values that do exist
        result._2.foreach {

            case   (  frequencyName, frequencyValue )   =>

            earsNormalFrequencies(frequencyName)  = frequencyValue

        }


        if ((earsNormal("leftEarIsCalculated") == false) && (result._1.getOrElse("ear",null).toLowerCase == "left"))  {

          if
            (
              (result._1.getOrElse("stimulus",null).toLowerCase ==  "air")        &&
              (result._2.getOrElse("f500",null)    !=  null)                      &&
              (result._2.getOrElse("f1000",null)   !=  null)                      &&
              (result._2.getOrElse("f2000",null)   !=  null)                      &&
              (result._2.getOrElse("f4000",null)   !=  null)
             ) {
            /*  Assume true to begin  */

            result._4("earIsNormal")  =   true

            result._4("earIsNormal")      =   calculateLeftEarIsNormal(earsNormalFrequencies)

            earsNormal("leftEarIsNormal")         =   if (result._4("earIsNormal")  == true)  { true  } else {false}
            earsNormal("leftEarIsCalculated")     =   true
          }

        }


        if ((earsNormal("rightEarIsCalculated") ==  false) && (result._1.getOrElse("ear",null).toLowerCase == "right")){

          if
            (
              (result._1.getOrElse("stimulus",null).toLowerCase ==  "air")        &&
              (result._2.getOrElse("f500",null)    !=  null)                      &&
              (result._2.getOrElse("f1000",null)   !=  null)                      &&
              (result._2.getOrElse("f2000",null)   !=  null)                      &&
              (result._2.getOrElse("f4000",null)   !=  null)
            ) {
            /*  Assume true to begin  */

            result._4("earIsNormal")  =   true

            result._4("earIsNormal")    =   calculateRightEarIsNormal(earsNormalFrequencies)

            earsNormal("rightEarIsNormal")         =   if (result._4("earIsNormal")  == true)  {  true  } else {false}
            earsNormal("rightEarIsCalculated")     =   true
          }



        }

      }


    }



  }

  def doLossSymmetry()  = {

    //Assume  Insufficient Data
    audiogramEncounterTestData._1("lossSymmetry")   = lossSymmetry("Insufficient Data")

    if (
          (earsNormal.getOrElse("leftEarIsNormal",null)     ==  true)               &&
          (earsNormal.getOrElse("rightEarIsNormal",null)    ==  true)
        )           {
      audiogramEncounterTestData._1("lossSymmetry")   = lossSymmetry("Normal")
    }
    else  if  (
                (earsNormal.getOrElse("leftEarIsNormal",null)   ==  true)           ||
                (earsNormal.getOrElse("rightEarIsNormal",null)  ==  true)
              )     {
      audiogramEncounterTestData._1("lossSymmetry")   = lossSymmetry("Unilateral")
    }
    else  if  (
                ((earsNormal.getOrElse("leftEarIsNormal",null)    ==  false))       &&
                ((earsNormal.getOrElse("rightEarIsNormal",null)  ==  false))
              )     {
      audiogramEncounterTestData._1("lossSymmetry")   = lossSymmetry("Bilateral")
    }


  }

  def doBetterEarWorseEar {

    var hasLeftEarPta:Boolean                 =   false

    var hasRightEarPta:Boolean                =   false

    var leftEarPta:BigDecimal                 =   0.0
    
    var rightEarPta:BigDecimal                =   0.0

    /*  Check to see if we have both left an right ear pta results  */
    for   (result <- audiogramEncounterResultData)  {

      if (hasLeftEarPta   ==  false)  {
        hasLeftEarPta   =
          (
            (result._1.getOrElse("ear",null).toLowerCase ==  "left")             &&
            (result._5.getOrElse("pta",null)  !=  null)
          )
        
        if (hasLeftEarPta)  { leftEarPta  =   result._5("pta")  }
      }


      if (hasRightEarPta   ==  false)  {
        hasRightEarPta   =
          (
            (result._1.getOrElse("ear",null).toLowerCase ==  "right")             &&
            (result._5.getOrElse("pta",null)  !=  null)
          )

        if (hasRightEarPta)  { rightEarPta  =   result._5("pta")  }
      }




    }
    
    if (hasLeftEarPta && hasRightEarPta)  {

      for   (result <- audiogramEncounterResultData)  {

        if ((result._1.getOrElse("ear",null).toLowerCase ==  "left") && (result._5.getOrElse("pta",null)  !=  null))  {

          if  (rightEarPta  ==  result._5("pta") ) {

            audiogramEncounterTestData._4("betterEarStatus")  =    betterEarStatusChoices("Same")
            audiogramEncounterTestData._6("worseEarId")       =    result._12("audiogramResultId")

          }
          else if (rightEarPta  > result._5("pta") ) {

            audiogramEncounterTestData._4("betterEarStatus")    =    betterEarStatusChoices("Left")
            audiogramEncounterTestData._5("betterEarId")        =    result._12("audiogramResultId")


          }
          else //if (rightEarPta  < result._5("pta") ) {
          {
            audiogramEncounterTestData._4("betterEarStatus")    =    betterEarStatusChoices("Right")
            audiogramEncounterTestData._6("worseEarId")         =    result._12("audiogramResultId")


          }


        }
        if ((result._1.getOrElse("ear",null).toLowerCase ==  "right") && (result._5.getOrElse("pta",null)  !=  null))  {

          if  (leftEarPta ==  result._5("pta")) {

            audiogramEncounterTestData._4("betterEarStatus")  =    betterEarStatusChoices("Same")
            audiogramEncounterTestData._5("betterEarId")      =    result._12("audiogramResultId")
          }
          else if (leftEarPta  <  result._5("pta")  )  {

            audiogramEncounterTestData._4("betterEarStatus")    =    betterEarStatusChoices("Left")
            audiogramEncounterTestData._6("worseEarId")         =    result._12("audiogramResultId")

          }
          else //if (leftEarPta  > result._5("pta") ) {
          {
            audiogramEncounterTestData._4("betterEarStatus")      =    betterEarStatusChoices("Right")
            audiogramEncounterTestData._5("betterEarId")          =    result._12("audiogramResultId")

          }


        }
        //  If everything is set exit the loop
        if  (
              (audiogramEncounterTestData._4.getOrElse("betterEarStatus",null) != null)  &&
              (audiogramEncounterTestData._5.getOrElse("betterEarStatus",null) != null)  &&
              (audiogramEncounterTestData._6.getOrElse("worseEarStatus",null)  != null)
            ) {
                return
        }

      }


    }
    else  {

      audiogramEncounterTestData._4("betterEarStatus")  =    betterEarStatusChoices("Insufficient Data")

      return
    }

  }

  def doTypeOfLoss:  Boolean    = {

    var isTypeOfLossCalculated                        =   false
    
    var hasSensorineuralLoss                          =   false
    
    var hasConductiveLoss                             =   false

    val leftAirResults: Map[String, Int]              =   Map.empty

    val rightAirResults: Map[String, Int]             =   Map.empty

    val leftBoneResults:Map[String, Int]              =   Map.empty

    val leftBoneResultsFlags:Map[String, String]      =   Map.empty

    val rightBoneResults:Map[String, Int]             =   Map.empty

    val rightBoneResultsFlags:Map[String, String]     =   Map.empty
    
    //Pull together all the data needed for calculations.
    //Only need flags for bone results
    for (result <-  audiogramEncounterResultData) {
      
      if  (  
            (result._1.getOrElse("ear",null).toLowerCase ==  "left" )               &&
            (result._1.getOrElse("stimulus",null).toLowerCase  ==  "air" )          &&
            (
              (result._2.getOrElse("f500",null)     !=  null )        &&
              (result._2.getOrElse("f1000",null)    !=  null )        &&
              (result._2.getOrElse("f2000",null)    !=  null )        &&
              (result._2.getOrElse("f4000",null)    !=  null )
            )
          )   {

              result._2.foreach( ( frequency ) => leftAirResults(frequency._1)  =  result._2(frequency._1) )

      }
      else if  (
                (result._1.getOrElse("ear",null).toLowerCase ==  "left" )               &&
                (result._1.getOrElse("stimulus",null).toLowerCase  ==  "bone" )         &&
                (
                    (result._2.getOrElse("f500",null)     !=  null )      ||
                    (result._2.getOrElse("f1000",null)    !=  null )      ||
                    (result._2.getOrElse("f2000",null)    !=  null )      ||
                    (result._2.getOrElse("f4000",null)    !=  null )
                )
              )     {

            result._2.foreach( ( frequency ) => leftBoneResults(frequency._1)  =  result._2(frequency._1) )

            result._3.foreach( ( frequencyFlag ) =>
              leftBoneResultsFlags(frequencyFlag._1)  =  result._3(frequencyFlag._1)

            )

      }
      else  if  (
                  (result._1.getOrElse("ear",null).toLowerCase ==  "right" )              &&
                  (result._1.getOrElse("stimulus",null).toLowerCase  ==  "air" )          &&
                (
                  (result._2.getOrElse("f500",null)     !=  null )        &&
                  (result._2.getOrElse("f1000",null)    !=  null )        &&
                  (result._2.getOrElse("f2000",null)    !=  null )        &&
                  (result._2.getOrElse("f4000",null)    !=  null )
                )
      )   {

            result._2.foreach( ( frequency ) => rightAirResults(frequency._1)  =  result._2(frequency._1) )

      }
      else if   (
                  (result._1.getOrElse("ear",null).toLowerCase ==  "right" )              &&
                  (result._1.getOrElse("stimulus",null).toLowerCase  ==  "bone" )         &&
                  (
                    (result._2.getOrElse("f500",null)     !=  null )      ||
                    (result._2.getOrElse("f1000",null)    !=  null )      ||
                    (result._2.getOrElse("f2000",null)    !=  null )      ||
                    (result._2.getOrElse("f4000",null)    !=  null )
                  )
                )     {

            result._2.foreach( ( frequency ) => rightBoneResults(frequency._1)  =  result._2(frequency._1) )

            result._3.foreach( ( frequencyFlag ) =>
              rightBoneResultsFlags(frequencyFlag._1)  =  result._3(frequencyFlag._1)

            )
      }

    }

    //  Enough Data for Calculation ?
    if  (
          (leftAirResults.isEmpty   && rightAirResults.isEmpty)       ||
          (
            (
              (!leftAirResults.isEmpty    && leftBoneResults.isEmpty)       &&
              (rightAirResults.isEmpty    || rightBoneResults.isEmpty)
            )                                                                 ||
            (
              (!rightAirResults.isEmpty   && rightBoneResults.isEmpty)      &&
              (leftAirResults.isEmpty     || leftBoneResults.isEmpty)
            )
          )                                                       ||
          (leftBoneResults.isEmpty  && rightBoneResults.isEmpty)
        )   {
      //Can Not Be Calculated
      return  isTypeOfLossCalculated      //  isTypeOfLossCalculated  = false

    }



    //  Sensorineural Loss  & Conductive Loss


    //Try Left Side
    if (!leftAirResults.isEmpty && !leftBoneResults.isEmpty)  {
      //  sensorineuralLoss
      if (!leftBoneResults.filter{ case (name,value)   =>  value > 15}.isEmpty ) {

          hasSensorineuralLoss     =   true

      }
      //  conductiveLoss
      leftAirResults.foreach( (frequency)  =>

        if (
              (
                (!(leftBoneResultsFlags.getOrElse(frequencyFlagMap(frequency._1),"").contains('N')) )       &&
                (!(leftBoneResultsFlags.getOrElse(frequencyFlagMap(frequency._1),"").contains('V')) )
              )                                                                                       &&
              (leftBoneResults.getOrElse(frequency._1,null)         !=  null)                         &&
              (!(frequency._2  <=  15))                                                               &&
              (!(frequency._1  ==  "f250" && frequency._2 <=  20))
           )     {


          if  ((frequency._2 - leftBoneResults(frequency._1)) >=  10) {

            hasConductiveLoss         =   true

            isTypeOfLossCalculated    =   true


          }

        }



      )

    }//Try Right Side Also
    if (!rightAirResults.isEmpty && !rightBoneResults.isEmpty)  {

      if (!rightBoneResults.filter{ case (name,value)   =>  value > 15}.isEmpty ) {

        hasSensorineuralLoss     =     true

      }
      //  conductiveLoss
      rightAirResults.foreach( (frequency)  =>

        if  (
              (
                (!(rightBoneResultsFlags.getOrElse(frequencyFlagMap(frequency._1),"").contains('N')) )       &&
                (!(rightBoneResultsFlags.getOrElse(frequencyFlagMap(frequency._1),"").contains('V')) )
              )                                                                                       &&
              (rightBoneResults.getOrElse(frequency._1,null)        !=  null)                         &&
              (!(frequency._2  <=  15))                                                               &&
              (!(frequency._1  ==  "f250" && frequency._2 <=  20))
            )     {

          //  If true gets over Written its OK
          if  ((frequency._2 - rightBoneResults(frequency._1)) >=  10) {

            hasConductiveLoss         =   true

            isTypeOfLossCalculated    =   true

          }

        }

      )

    }
    else  { return       isTypeOfLossCalculated }

    //Only started out will false for both so they can be set without conditions

    audiogramEncounterTestData._2("hasConductiveLoss")        =   hasConductiveLoss

    audiogramEncounterTestData._3("hasSensorineuralLoss")     =   hasSensorineuralLoss

    isTypeOfLossCalculated                                    =   true

    return       isTypeOfLossCalculated

  }

  def calculateLeftEarIsNormal(values: Map[String, Int]):  Boolean  =  {
    //This only gets calculated if both ear's have the appropriate frequency data for air
    //Must be calculated after the full encounter data is parsed.
    values.foreach  { case  (frequencyName, frequencyValue)  =>

      //Return false with the first sign of abnormality

      if ((frequencyName ==  "f500") || (frequencyName ==  "f1000") || (frequencyName ==  "f2000") || (frequencyName ==  "f4000")) {

        if (  !(frequencyValue <= 15))  { return false  }
      }
      else  if (  !(frequencyValue <= 20))   {  return false    }


      /*
        //Not sure why the Python Code is not checking for null in the previous if statement as well
      else if (frequencyValue  != null)    {

        if (  !(frequencyValue <= 20))  { return false  }
      }
      */

    }

    true
  }

  def calculateRightEarIsNormal(values: Map[String, Int]):  Boolean =  {
    //This only gets calculated if both ear's have the appropriate frequency data for air
    //Must be calculated after the full encounter data is parsed.
    values.foreach  { case  (frequencyName, frequencyValue)  =>

      //Return false with the first sign of abnormality

      if ((frequencyName ==  "f500") || (frequencyName ==  "f1000") || (frequencyName ==  "f2000") || (frequencyName ==  "f4000")) {

        if (  !(frequencyValue <= 15))  { return false  }
      }
      else if (  !(frequencyValue <= 20))  { return false  }



      /*
        //Not sure why the Python Code is not checking for null in the previous if statement as well
      else if (frequencyValue  != null)    {

        if (  !(frequencyValue <= 20))  { return false  }
      }
      */


    }

    true
  }

  def calculateSeverities(pta: BigDecimal,  ptaWorse: Boolean):   (Boolean, String)   = {

    var   isWorseSeverity:    Boolean     =   false
    
    var   severity: String                =   null

    if ((pta < 91)   && (ptaWorse  == true))    {

      //pta_worse implies unknown severity

      severity            =   severityChoices("Insufficient Data")
      isWorseSeverity     =   true

    }
    else if (pta <= 15)                         {

      severity            =   severityChoices("Normal")

    }
    else if ((pta > 15) && (pta <= 25))         {

      severity            =   severityChoices("Slight")

    }
    else if ((pta > 25) && (pta <= 40))         {

      severity            =   severityChoices("Mild")

    }
    else if ((pta > 40) && (pta <= 55))         {

      severity            =   severityChoices("Moderate")

    }
    else if ((pta > 55) && (pta <= 70))         {

      severity            =   severityChoices("Moderately Severe")

    }
    else if ((pta > 70) && (pta <= 90))         {

      severity            =   severityChoices("Severe")

    }
    else if (pta > 90)                          {

      severity            =   severityChoices("Profound")

    }
    
    return  (isWorseSeverity,severity)

  }

  def calculateShapes(shapesFrequencies:MutableList[Int],shapesFrequenciesFlags:MutableList[String]): String   =  {
    
    var   shape:  String    =   null

    //No Responses
    shapesFrequenciesFlags.foreach (  flag => if (flag.contains('N')) {

      return  shapeChoices("Has NRs")

    }

    )

    //  < 30 dB
    if (shapesFrequencies.filter(i => i >= 30).isEmpty )  {
      shape   = shapeChoices("All <30")
      return  shape
    }

    //Sloping
    if ((shapesFrequencies.last  - shapesFrequencies(0))  > 20 )  {

      breakable {

        for (j <- 0 to (shapesFrequencies.length  - 2)){  if ((shapesFrequencies(j + 1) - shapesFrequencies(j)) < 0)  {
                                                              break
                                                          }
        }
        shape   = shapeChoices("Sloping")
        return  shape
      }
    }

    //Rising
    if ((shapesFrequencies(0) - shapesFrequencies.last)   > 20 )  {

      breakable {

        for (j <- 0 to (shapesFrequencies.length  - 2)){  if ((shapesFrequencies(j) -  shapesFrequencies(j + 1) ) < 0){
                                                            break
                                                          }
        }
        shape   = shapeChoices("Rising")
        return  shape
      }
    }

    //Flat
    if ((shapesFrequencies.max - shapesFrequencies.min)   <=   20 )  {
      shape     = shapeChoices("Flat")
      return  shape
    }

    //U-shaped

    if ( ! shapesFrequencies.slice(1,(shapesFrequencies.length - 1)).filter(i =>
            (
              ( i -   shapesFrequencies.slice(1,(shapesFrequencies.length - 1))(0)    )         >= 20
            )
              ||
            (
              ( i -   shapesFrequencies.slice(1,(shapesFrequencies.length - 1)).last  )         >= 20
            )
          ).isEmpty
    )  {
      shape   = shapeChoices("U-Shaped")
      return  shape
    }


    //Tent-shaped

    if ( ! shapesFrequencies.slice(1,(shapesFrequencies.length - 1)).filter(i =>
            (
              ( shapesFrequencies.slice(1,(shapesFrequencies.length - 1))(0)   - i    )         >= 20
            )
              ||
            (
              ( shapesFrequencies.slice(1,(shapesFrequencies.length - 1)).last - i    )         >= 20
            )
    ).isEmpty
    )  {
      shape   = shapeChoices("Tent-Shaped")
      return  shape
    }


    return shapeChoices("Other")



  }

  override def clear:  Boolean = {

    earsNormal    =   Map.empty

    audiogramEncounterResultData      = MutableList.empty

    audiogramEncounterTestData        = (Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty,Map.empty)

    true
  }

  override def close:  Boolean  = {

    log.close()
    processBackend.close()
    sourceBackend.close()
    targetBackend.close()

    true
  }


}


object AudiogramTransform  {

  val defaultSourceTableNames: Seq[String]      = Seq("concept_aggregate","staging_encounter","core_datasource")

  var defaultTargetTableNames: Seq[String]      = Seq("staging_audiogramtest","staging_audiogramresult")
  /*****  Temporary   ******/
  val testingSourceTableNames: Seq[String]      = Seq("concept_aggregate","v_staging_encounter","v_core_datasource")

  defaultTargetTableNames                       = testingSourceTableNames
  /*****              ******/
  val defaultSourceSchemaName                   = "qe11b"

  val defaultTargetSchemaName                   = "qe11c"

  val defaultSourcePropertiesFP                 =
    "conf/connection-properties/transform-source.properties"

  val defaultTargetPropertiesFP                 =
    "conf/connection-properties/transform-target.properties"


  def apply(): AudiogramTransform   =   {

    try {

      new AudiogramTransform  (
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

  def apply(query: String): AudiogramTransform  =   {

    try {

      new AudiogramTransform  (
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

