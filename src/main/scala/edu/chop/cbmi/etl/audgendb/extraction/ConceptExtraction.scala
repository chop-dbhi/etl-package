package edu.chop.cbmi.etl.audgendb.extraction

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/15/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */

/* Load public.production_produceduredetail@augendb_production_data from public.staging_produceduredetail@augendb_staging */


import edu.chop.cbmi.etl.library.transformers.aggregators.aggregatorA
import edu.chop.cbmi.etl.library.transformers.aggregators.aggregatorB

import edu.chop.cbmi.dataExpress.dataModels.{DataRow,DataTable}

import edu.chop.cbmi.dataExpress.dataModels.DataType
import edu.chop.cbmi.dataExpress.dataModels.sql.IntegerDataType
import util.Random
import collection.mutable.MutableList


case class ConceptExtraction(  override val sdbfp: String,
                               override val tdbfp: String,
                               override val sourceTableName:  Seq[String],
                               override val targetTableName:  Seq[String],
                               override val sourceDbSchemaName:  Seq[Option[String]],
                               override val targetDbSchemaName:  Seq[Option[String]],
                               override val query: Option[String])
  extends AudgendbSqlExtraction(sdbfp,tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {

  var conceptAggregateTableName   =   "concept_aggregate"

  md5TargetTableName              =   conceptAggregateTableName


  override def md5PerformanceIndex(operation: String):  Boolean     =   {
    //Performance index for adding md5 to concept_aggregate table
    if (operation == "add")   {

      pat_enc_csn_id__concept_idIndex("add")

    }
    else  if (operation == "drop")  {

      pat_enc_csn_id__concept_idIndex("drop")

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  def pat_enc_csn_id__concept_idIndex(operation: String):  Boolean  =   {


    val   indexColumnNames              =     Seq("pat_enc_csn_id","concept_id")

    val   indexName                     =     "%s_%s_idx_01".format(indexColumnNames(0),indexColumnNames(1))

    if (operation == "add") {

      processQuery =
        if (targetDefaultSchemaName.isEmpty)  {

          """
          create unique index %s on %s using btree (%s,%s)
          """.format(indexName,conceptAggregateTableName,indexColumnNames(0),indexColumnNames(1))

        }
        else  {

          """
          create unique index %s on %s.%s using btree (%s,%s)
          """.format(indexName,targetDefaultSchemaName.get,conceptAggregateTableName,indexColumnNames(0),indexColumnNames(1))

        }


      targetBackend.execute(processQuery)

      targetBackend.commit()

      return true

    }
    else  if (operation == "drop")  {

      processQuery =
        """
        select
        c.relname  as "index_name"
		    from
		    pg_catalog.pg_class c,
		    pg_catalog.pg_namespace n,
		    pg_catalog.pg_index i,
		    pg_catalog.pg_class t
		    where
		    n.oid         =       c.relnamespace					        and
		    i.indexrelid  =       c.oid							              and
		    i.indrelid    =       t.oid							              and
		    c.relkind     =       'i'									            and
		    n.nspname     =       '%s'		                        and
		    t.relname     =       '%s'                            and
		    c.relname     =       '%s'
		    order by
		    n.nspname,t.relname,c.relname
        """.format(targetDefaultSchemaName.get,  conceptAggregateTableName,indexName)

      val result  = targetBackend.executeQuery(processQuery)

      if (result.next)  {

        processQuery   =
          """
          drop index if exists %s.%s restrict
          """.format(targetDefaultSchemaName.get,result.getString("index_name"))

        targetBackend.execute(processQuery)

        targetBackend.commit()

      }

      return true

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }


  }

  def aggregate(targetAggregateTableName:Option[String]  = Option(conceptAggregateTableName))  :Boolean  = {

    aggregateWithB(targetAggregateTableName)
    
  }

  def aggregateWithA(targetAggregateTableName:  Option[String] = None  ): Boolean       =       {

    var   aggregatedRowToInsert: List[Any]    =   List.empty

    var   currentRow:  DataRow[Any]           =   DataRow(  ("pat_enc_csn_id",""),
                                                            ("concept_id",""),
                                                            ("line",""),
                                                            ("concept_value",""),
                                                            ("extract_date",""),
                                                            ("md5","")
                                                  )

    lazy val sTableName                     =   targetTableName(0)

    lazy val tTableName                     =   targetAggregateTableName.getOrElse(conceptAggregateTableName)

    lazy val sDbSchemaName                  =   targetDbSchemaName(0)

    lazy val tDbSchemaName                  =   targetDbSchemaName(0)

    val defaultLineNumber                   =   1

    processQuery    =
      if (sDbSchemaName.isEmpty)  {
        """
        SELECT CAST (pat_enc_csn_id AS VARCHAR(20)),concept_id, CAST (line AS VARCHAR(40)),
        CAST(concept_value AS VARCHAR(50000)),CAST(extract_date AS VARCHAR(40)),md5 FROM
        %s
        ORDER BY pat_enc_csn_id,concept_id, line ASC
        """.format(sTableName)    //ORDER IS MANDATORY
      }
      else  {
        """
        SELECT CAST (pat_enc_csn_id AS VARCHAR(20)),concept_id, CAST (line AS VARCHAR(40)),
        CAST(concept_value AS VARCHAR(50000)),CAST(extract_date AS VARCHAR(40)),md5 FROM
        %s.%s
        ORDER BY pat_enc_csn_id,concept_id, line ASC
        """.format(sDbSchemaName.get,sTableName)    //ORDER IS MANDATORY
      }


    lazy val sData                                    =   DataTable(targetBackend,processQuery)

    lazy val dataTypes:List[DataType]                 =   sData.dataTypes.toList.+:(IntegerDataType())

    lazy val aggregators: Seq[String]                 =   Seq("pat_enc_csn_id","concept_id")

    lazy val columnToAggregate: String                =   "concept_value"

    lazy val aggregator                               =   aggregatorA(aggregators,columnToAggregate)

    var  id                                           =   1

    targetBackend.connection.setAutoCommit(true)

    targetBackend.createTable(
      tTableName,sData.column_names.toList.+:("id"),dataTypes,tDbSchemaName
    )

    sData.foreach { row =>

      aggregator.aggregate(row)

      if (aggregator.newAggregation && !aggregator.initialize)  {
        //  If a new aggregation has just been started and this is not the initial aggregation
        //  save the aggregation that was just completed

        aggregatedRowToInsert     =           List(     (id),
          (currentRow("pat_enc_csn_id").getOrElse(null)),
          (currentRow("concept_id").getOrElse(null)),
          (defaultLineNumber),
          //(currentRow("line").getOrElse(null)),
          (aggregator.getAggregation),
          (currentRow("extract_date").getOrElse(null)),
          (currentRow("md5").getOrElse(null))
        )

        //Create a datable that is bulk insert-able using the dataRow
        val rowToBulkInsert       =   DataTable(sData.column_names.+:("id"),aggregatedRowToInsert)
        //Add the prepared row to the bulk insert batch
        targetBackend.batchInsert(tTableName,rowToBulkInsert,tDbSchemaName )

        id+=1
      }

      //The row to be inserted is current Row - 1
      currentRow = row



    }

    //Cleanup:

    //    There will always be one row (concept_value) left in the aggregator
    //  - If A new Aggregation was started (aggregator.newAggregation == true)  with the very last row (concept_value),
    //    stop aggregation ( this will set aggregation  = currentAggregation) and insert that final aggregation
    //    ( from the one row (concept_value)
    //  - If the (aggregator.newAggregation == false)    , then the last row accepted by the aggregator completes
    //    the current aggregation.  So stop aggregation and insert the row

    aggregator.stopAggregation()


    aggregatedRowToInsert     =           List(     (id),
                                                    (currentRow("pat_enc_csn_id").getOrElse(null)),
                                                    (currentRow("concept_id").getOrElse(null)),
                                                    (defaultLineNumber),
                                                    //(currentRow("line").getOrElse(null)),
                                                    (aggregator.getAggregation),
                                                    (currentRow("extract_date").getOrElse(null)),
                                                    (currentRow("md5").getOrElse(null))
                                          )


    val rowToBulkInsert       =   DataTable(sData.column_names.+:("id"),aggregatedRowToInsert)

    targetBackend.batchInsert(tTableName,rowToBulkInsert,tDbSchemaName )


    true

  }

  def aggregateWithB(targetAggregateTableName:  Option[String]  = None  ) :Boolean      =       {
    //May need to use scala -J-Xmx1200m (at least) to run this one
    lazy val    sIdentifierQuote                =   sourceBackend.sqlDialect.quoteIdentifier("")
    //Use the target Table of the extraction as the source table for the aggregation
    lazy val    sTableName                      =   targetTableName(0)

    lazy val    tTableName                      =   targetAggregateTableName.getOrElse(conceptAggregateTableName)

    lazy val    sDbSchemaName                   =   targetDbSchemaName(0)

    lazy val    tDbSchemaName                   =   targetDbSchemaName(0)


    processQuery    =
      if (sDbSchemaName.isEmpty)  {
              """
              SELECT CAST (pat_enc_csn_id AS VARCHAR(20)),concept_id, CAST (line AS VARCHAR(40)),
              CAST(concept_value AS VARCHAR(50000)),CAST(extract_date AS VARCHAR(40)),md5 FROM
              %s
              ORDER BY pat_enc_csn_id,concept_id, line ASC
              """.format(sTableName)    //ORDER IS MANDATORY
      }
      else  {
              """
              SELECT CAST (pat_enc_csn_id AS VARCHAR(20)),concept_id, CAST (line AS VARCHAR(40)),
              CAST(concept_value AS VARCHAR(50000)),CAST(extract_date AS VARCHAR(40)),md5 FROM
              %s.%s
              ORDER BY pat_enc_csn_id,concept_id, line ASC
              """.format(sDbSchemaName.get,sTableName)    //ORDER IS MANDATORY
      }


    lazy val conceptTable                       =   DataTable(targetBackend,processQuery)

    lazy val dataTypes:List[DataType]           =   conceptTable.dataTypes.toList.+:(IntegerDataType())

    targetBackend.connection.setAutoCommit(true)

    targetBackend.createTable(
      tTableName,conceptTable.column_names.toList.+:("id"),dataTypes,tDbSchemaName
    )

    lazy val aggregator                     =   aggregatorB(conceptTable,tTableName,tDbSchemaName,targetBackend)

    aggregator.aggregate

    true
  }


}


object ConceptExtraction  {

  val clarityAudgenDbPatientTableName           = "audgendb_patient"

  val clarityAudgenDbEncounterTableName         = "audgendb_encounter"

  val defaultSourceTableName                    = "PAT_ENC_CONCEPT"

  val defaultTargetTableName                    = "concept"

  val defaultSourceSchemaName                   = "RESEARCH"

  val defaultTargetSchemaName                   = "qe11b"

  val defaultSourcePropertiesFP                       =
    "conf/connection-properties/extraction-source.properties"

  val defaultTargetPropertiesFP                       =
    "conf/connection-properties/extraction-target.properties"

  def buildQuery(): String = {

    val query:String  =
    """
    SELECT
    "PAT_ENC_CONCEPT"."PAT_ENC_CSN_ID" "pat_enc_csn_id", "CONCEPT_ID" "concept_id",
    "LINE" "line", "CONCEPT_VALUE" "concept_value",
    TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SSTZH":"TZM') "extract_date",
    CAST(NULL AS CHAR(150)) AS "md5"
    FROM
    "PAT_ENC_CONCEPT" , "audgendb_encounter"
    WHERE
    "PAT_ENC_CONCEPT"."PAT_ENC_CSN_ID" = "audgendb_encounter"."pat_enc_csn_id" AND
    "PAT_ENC_CONCEPT"."CONCEPT_ID" LIKE 'CHOP#AUDIO%'
    ORDER BY "PAT_ENC_CONCEPT"."PAT_ENC_CSN_ID",
    "PAT_ENC_CONCEPT"."CONCEPT_ID", "PAT_ENC_CONCEPT"."LINE" ASC
    """

    query

  }


  def apply(): ConceptExtraction = {

    try {

      new ConceptExtraction (
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



  def apply(query: String): ConceptExtraction = {

    try {

      new ConceptExtraction (
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

