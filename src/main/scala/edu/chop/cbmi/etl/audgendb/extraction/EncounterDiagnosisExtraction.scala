package edu.chop.cbmi.etl.audgendb.extraction

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */



case class EncounterDiagnosisExtraction(    override val sdbfp: String,
                                            override val tdbfp: String,
                                            override val sourceTableName:  Seq[String],
                                            override val targetTableName:  Seq[String],
                                            override val sourceDbSchemaName:  Seq[Option[String]],
                                            override val targetDbSchemaName:  Seq[Option[String]],
                                            override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp,tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {


  override  def  md5:Boolean  = {

    md5PerformanceIndex("drop")     //Will Only drop if exist

    md5PerformanceIndex("add")

    super.md5()

  }

  override def md5PerformanceIndex(operation: String):  Boolean     =   {
    //Performance index for adding md5 to medical_history table
    if (operation == "add")   {

      encounter_diagnosisIndex01("add")

    }
    else  if (operation == "drop")  {

      encounter_diagnosisIndex01("drop")

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  override def performanceIndex(operation: String):       Boolean           =   {
    //Performance index for adding md5 to medical_history table
    if (operation == "add")   {

      encounter_diagnosisIndex01("add")

    }
    else  if (operation == "drop")  {

      encounter_diagnosisIndex01("drop")

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  def encounter_diagnosisIndex01(operation: String):  Boolean  =   {

    val   indexColumnNames              =
      Seq("pat_id","pat_enc_csn_id","icd9_code")

    val   indexName                     =   "encounter_diagnosis_md5_performance_idx_01"

    if (operation == "add") {

      processQuery =
        if (targetDefaultSchemaName.isEmpty)  {

          """
          create index %s on %s using btree (%s,%s,%s)
          """.format( indexName,targetTableName(0),indexColumnNames(0),indexColumnNames(1),
                      indexColumnNames(2))

        }
        else  {

          """
          create index %s on %s.%s using btree (%s,%s,%s)
          """.format( indexName,targetDefaultSchemaName.get,targetTableName(0),indexColumnNames(0),indexColumnNames(1),
                      indexColumnNames(2))
        }


      targetBackend.execute(processQuery)

      targetBackend.commit()

      return true

    }
    else if (operation == "drop")  {

      processQuery   =
        """
        drop index if exists %s.%s restrict
        """.format(targetDefaultSchemaName.get,indexName)

      targetBackend.execute(processQuery)

      targetBackend.commit()

      return true

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }


  }


}


object EncounterDiagnosisExtraction  {

  val clarityAudgenDbPatientTableName                   = "audgendb_patient"

  val defaultSourceTableName                            = "PAT_ENC_DX"

  val defaultSourceSchemaName                           = "RESEARCH"

  val defaultTargetSchemaName                           = "qe11b"

  val defaultTargetTableName                            = "encounter_diagnosis"

  val defaultSourcePropertiesFP                         =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP                         =
    "conf/connection-properties/extraction-target.properties"

  def buildQuery(): String = {


    val query: String =
    """
    SELECT
    pd."pat_id",
    pd."pat_enc_csn_id",
    pd."line",
    pd."contact_date",
    pd."icd9_code",
    pd."annotation",
    pd."comments",
    pd."primary_dx_yn",
    CURRENT_TIMESTAMP as "extract_date",
    CAST(NULL AS CHAR(150)) AS "md5"
    FROM
    pat_enc_dx pd,
    audgendb_pat_id_list a
    WHERE
    a.pat_id = pd.pat_id
    order by
    pd.pat_id asc
    """

    query
  }


  def apply(): EncounterDiagnosisExtraction = {

    try {

      new EncounterDiagnosisExtraction (
        defaultSourcePropertiesFP,            defaultTargetPropertiesFP,
        Seq(defaultSourceTableName),          Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(buildQuery())
      )
    }
    catch {
      case e:RuntimeException     => throw new RuntimeException
    }

  }



  def apply(query: String): EncounterDiagnosisExtraction = {

    try {

      new EncounterDiagnosisExtraction   (
        defaultSourcePropertiesFP,            defaultTargetPropertiesFP,
        Seq(defaultSourceTableName),          Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(query)
      )
    }
    catch {
      case e:RuntimeException     => throw new RuntimeException
    }

  }


}

