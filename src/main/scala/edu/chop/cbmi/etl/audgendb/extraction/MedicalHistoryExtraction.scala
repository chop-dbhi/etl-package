package edu.chop.cbmi.etl.audgendb.extraction

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */




case class MedicalHistoryExtraction(  override val sdbfp: String,
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

  override def md5PerformanceIndex(operation: String):    Boolean           =   {
    //Performance index for adding md5 to medical_history table
    if (operation == "add")   {

      medical_historyIndex01("add")

    }
    else  if (operation == "drop")  {

      medical_historyIndex01("drop")

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  override def performanceIndex(operation: String):       Boolean           =   {
    //Performance index for adding md5 to medical_history table
    if (operation == "add")   {

      medical_historyIndex01("add")

    }
    else  if (operation == "drop")  {

      medical_historyIndex01("drop")

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  def medical_historyIndex01(operation: String):          Boolean           =   {

    val   indexColumnNames              =     Seq("pat_id","pat_enc_csn_id","icd9_code")

    val   indexName                     =     "medical_history_md5_performance_idx_01"

    if (operation == "add") {

      processQuery =
        if (targetDefaultSchemaName.isEmpty)  {

          """
          create index %s on %s using btree (%s,%s,%s)
          """.format(indexName,targetTableName(0),indexColumnNames(0),indexColumnNames(1),indexColumnNames(2))

        }
        else  {

          """
          create index %s on %s.%s using btree (%s,%s,%s)
          """.format(indexName,targetDefaultSchemaName.get,targetTableName(0),indexColumnNames(0),indexColumnNames(1),indexColumnNames(2))

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


object MedicalHistoryExtraction  {

  val clarityAudgenDbPatientTableName                   = "audgendb_patient"

  val defaultSourceTableName                            = "MEDICAL_HX"

  val defaultSourceSchemaName                           = "RESEARCH"

  val defaultTargetSchemaName                           = "qe11b"

  val defaultTargetTableName                            = "medical_history"

  val defaultSourcePropertiesFP                         =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP                         =
    "conf/connection-properties/extraction-target.properties"

  def buildQuery(): String = {


    val query: String =
    """
    SELECT
    mh."pat_id",
    mh."pat_enc_csn_id",
    mh."line",
    mh."icd9_code",
    mh."medical_hx_date",
    mh."comments",
    mh."med_hx_annotation",
    mh."contact_date",
    CURRENT_TIMESTAMP as "extract_date",
    CAST(NULL AS CHAR(150)) AS "md5"
    FROM
    medical_hx mh,
    audgendb_pat_id_list a
    WHERE
    a.pat_id = mh.pat_id
    """

    query
  }


  def apply(): MedicalHistoryExtraction = {

    try {

      new MedicalHistoryExtraction (
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



  def apply(query: String): MedicalHistoryExtraction = {

    try {

      new MedicalHistoryExtraction   (
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

