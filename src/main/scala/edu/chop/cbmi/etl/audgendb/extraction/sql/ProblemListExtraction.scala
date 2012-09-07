package edu.chop.cbmi.etl.audgendb.extraction.sql

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */


case class ProblemListExtraction(override val sdbfp: String,
                                 override val tdbfp: String,
                                 override val sourceTableName: Seq[String],
                                 override val targetTableName: Seq[String],
                                 override val sourceDbSchemaName: Seq[Option[String]],
                                 override val targetDbSchemaName: Seq[Option[String]],
                                 override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp, tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {


  override def md5: Boolean = {

    md5PerformanceIndex("drop") //Will Only drop if exist

    md5PerformanceIndex("add")

    super.md5()

  }

  override def md5PerformanceIndex(operation: String): Boolean = {
    //Performance index for adding md5 to medical_history table
    if (operation == "add") {

      problemlistIndex01("add")

    }
    else if (operation == "drop") {

      problemlistIndex01("drop")

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  override def performanceIndex(operation: String): Boolean = {
    //Performance index for adding md5 to medical_history table
    if (operation == "add") {

      problemlistIndex01("add")

    }
    else if (operation == "drop") {

      problemlistIndex01("drop")

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  def problemlistIndex01(operation: String): Boolean = {

    val indexColumnNames = Seq("problem_list_id", "pat_id", "icd9_code")

    val indexName =
      "problemlist_md5_performance_idx_01".format(indexColumnNames(0), indexColumnNames(1), indexColumnNames(2))

    if (operation == "add") {

      processQuery =
        if (targetDefaultSchemaName.isEmpty) {

          """
          create index %s on %s using btree (%s,%s,%s)
          """.format(indexName, targetTableName(0), indexColumnNames(0), indexColumnNames(1), indexColumnNames(2))

        }
        else {

          """
          create index %s on %s.%s using btree (%s,%s,%s)
          """.format(indexName, targetDefaultSchemaName.get, targetTableName(0), indexColumnNames(0), indexColumnNames(1), indexColumnNames(2))

        }


      targetBackend.execute(processQuery)

      targetBackend.commit()

      return true

    }
    else if (operation == "drop") {

      processQuery =
        """
        drop index if exists %s.%s restrict
        """.format(targetDefaultSchemaName.get, indexName)

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


object ProblemListExtraction {

  val clarityAudgenDbPatientTableName = "audgendb_patient"

  val defaultSourceTableName = "PROBLEM_LIST"

  val defaultSourceSchemaName = "RESEARCH"

  val defaultTargetSchemaName = "qe11b"

  val defaultTargetTableName = "problemlist"

  val defaultSourcePropertiesFP =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP =
    "conf/connection-properties/extraction-target.properties"

  def buildQuery(): String = {


    val query: String =
      """
      SELECT
      distinct
      pl."problem_list_id",
      pl."pat_id",
      pl."icd9_code",
      pl."description",
      pl."noted_date",
      pl."resolved_date",
      pl."status",
      pl."problem_cmt",
      CURRENT_TIMESTAMP as "extract_date",
      CAST(NULL AS CHAR(150)) AS "md5"
      FROM
      problem_list pl,
      audgendb_pat_id_list a
      WHERE
      a.pat_id = pl.pat_id                                  AND
      decode(pl.status,'Deleted','Deleted','Not Deleted')   <> 'Deleted'
      """

    query
  }


  def apply(): ProblemListExtraction = {

    try {

      new ProblemListExtraction(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        Seq(defaultSourceTableName), Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(buildQuery())
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }


  def apply(query: String): ProblemListExtraction = {

    try {

      new ProblemListExtraction(
        defaultSourcePropertiesFP, defaultTargetPropertiesFP,
        Seq(defaultSourceTableName), Seq(defaultTargetTableName),
        Seq(Option(defaultSourceSchemaName)), Seq(Option(defaultTargetSchemaName)),
        Option(query)
      )
    }
    catch {
      case e: RuntimeException => throw new RuntimeException
    }

  }


}

