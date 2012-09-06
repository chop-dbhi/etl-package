package edu.chop.cbmi.etl.audgendb.extraction

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 7/13/12
 * Time: 11:09 AM
 * To change this template use File | Settings | File Templates.
 */


import edu.chop.cbmi.dataExpress.dataModels.DataTable
import edu.chop.cbmi.dataExpress.dataModels.DataType
import edu.chop.cbmi.dataExpress.dataModels.sql._
import edu.chop.cbmi.dataExpress.backends.SqlBackendFactory

import edu.chop.cbmi.etl.extraction.sql.SqlExtraction

import edu.chop.cbmi.etl.util.FileProperties

import scala.collection.mutable.MutableList
import scala.collection.mutable.Map
import scala.collection.Map
import scala.util.Random

case class AudgendbSqlExtraction (  override val sdbfp: String,
                            override val tdbfp: String,
                            override val sourceTableName:  Seq[String],
                            override val targetTableName:  Seq[String],
                            override val sourceDbSchemaName:  Seq[Option[String]],
                            override val targetDbSchemaName:  Seq[Option[String]],
                            override val query: Option[String])
  extends SqlExtraction(sdbfp,tdbfp, sourceTableName, targetTableName, sourceDbSchemaName, targetDbSchemaName, query) {

  val md5Backend            =       SqlBackendFactory((new FileProperties(tdbfp)).props)

  var md5TargetTableName    =       targetDefaultTableName

  md5Backend.connect

  lazy val targetColumns:scala.collection.Map[String,DataType]        =     columns(targetDefaultTableName)

  val md5ColumnName                                                   =     "md5"

  val md5ExcludeColumns:Seq[String]                                   =     Seq(md5ColumnName,"extract_date")

  val md5NullCharacterValueSubstitute:String                          =     "'5000'"

  val md5NullDecimalValueSubstitute:String                            =     "5000"

  val md5NullIntegerValueSubstitute:String                            =     "5000"

  val md5NullDateValueSubstitute:String                               =     "CURRENT_DATE"

  val md5NullDateTimeValueSubstitute:String                           =     "CURRENT_DATE"

  val md5NullTimeStampValueSubstitute:String                          =     "LOCAL_TIMESTAMP"

  def md5():  Boolean  = {
    //Some Columns have null values
    //The Md5 is null for any row that has a null value even when type casting to varchar is done
    //So the columns that have null values are being excluded from the md5 calculation on each row.
    //md5ExcludeColumn are excluded from md5 calculation
    //Escape quotes are applied to the source data literals so that they don't break the where clause in
    //the md5 update query (md5Query)
    val   columnNames   =   columns(md5TargetTableName).keys.filterNot(columnName => md5ExcludeColumns.contains(columnName))

    val md5Calculator  =
        "md5(%s)".format(
          ( for(name:String <- columnNames )
            yield {
            //May need to consider more of these
            if        (targetColumns(name).toString.slice(0,targetColumns(name).toString.indexOf("("))    ==  CharacterDataType.toString  )     {
              "CAST( (CASE WHEN %s is null THEN %s ELSE %s END) AS VARCHAR)  || ".format(name,md5NullCharacterValueSubstitute,name)
            }
            else  if (targetColumns(name).toString.slice(0,targetColumns(name).toString.indexOf("("))    ==   IntegerDataType.toString  )       {
              "CAST( (CASE WHEN %s is null THEN %s ELSE %s END) AS VARCHAR)  || ".format(name,md5NullIntegerValueSubstitute,name)
            }
            else  if (targetColumns(name).toString.slice(0,targetColumns(name).toString.indexOf("("))    ==   DecimalDataType.toString  )       {
              "CAST( (CASE WHEN %s is null THEN %s ELSE %s END) AS VARCHAR)  || ".format(name,md5NullDecimalValueSubstitute,name)
            }
            else  if (targetColumns(name).toString.slice(0,targetColumns(name).toString.indexOf("("))    ==   DateDataType.toString  )          {
              "CAST( (CASE WHEN %s is null THEN %s ELSE %s END) AS VARCHAR)  || ".format(name,md5NullDateValueSubstitute,name)
            }
            else  if (targetColumns(name).toString.slice(0,targetColumns(name).toString.indexOf("("))    ==   DateTimeDataType.toString  )      {
              "CAST( (CASE WHEN %s is null THEN %s ELSE %s END) AS VARCHAR)  || ".format(name,md5NullDateTimeValueSubstitute,name)
            }
            else  if (targetColumns(name).toString.slice(0,targetColumns(name).toString.indexOf("("))    ==   TimeDataType.toString  )          {
              "CAST( (CASE WHEN %s is null THEN %s ELSE %s END) AS VARCHAR)  || ".format(name,md5NullTimeStampValueSubstitute,name)
            }
            else                                                                                                                                {
              "CAST( (CASE WHEN %s is null THEN %s ELSE %s END) AS VARCHAR)  || ".format(name,md5NullCharacterValueSubstitute,name)
            }




          }
            ).mkString.dropRight(3)
        )


    val md5Query =
        if (targetDefaultSchemaName.isEmpty)
          "UPDATE %s SET %s = %s ".format(targetDefaultTableName,md5ColumnName,md5Calculator)
        else
          "UPDATE %s.%s SET %s = %s ".format(targetDefaultSchemaName.get,md5TargetTableName,md5ColumnName,md5Calculator)

    processQuery = md5Query

    try {

        md5Backend.execute(processQuery)

        md5Backend.commit()

    }
    catch {

        case e:java.sql.SQLException  =>    {

          close

          println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

          throw e
        }
    }


    md5NotNull

  }

  def md5SkipNull():  Boolean  = {
    //Some Columns have null values
    //The Md5 is null for any row that has a null value even when type casting to varchar is done
    //So the columns that have null values are being excluded from the md5 calculation on each row.
    //md5ExcludeColumn are excluded from md5 calculation
    //Escape quotes are applied to the source data literals so that they don't break the where clause in
    //the md5 update query (md5Query)
    val   columnNames   =   columns(md5TargetTableName).keys.filterNot(columnName => md5ExcludeColumns.contains(columnName))

    val   rowList:MutableList[(String, Any)]                =   MutableList.empty

    val quotedColumnLiterals    = (
      for (name:String <- columnNames)     yield {

        "quote_literal(%s) as %s".format(name,name)

      }
      ).mkString(",")

    processQuery  =
      if (targetDefaultSchemaName.isEmpty)  {

        "SELECT %s FROM %s ".format(quotedColumnLiterals,md5TargetTableName)

      }
      else  {

        "SELECT %s FROM %s.%s ".format(quotedColumnLiterals,targetDefaultSchemaName.get,md5TargetTableName)

      }

    val md5Data           =       DataTable(targetBackend,processQuery)

    md5Data.foreach { row =>

      val md5Calculator  =
        "md5(%s)".format(
          ( for(name:String <- columnNames.filter(
            columnName => (row(columnName).getOrElse("")  != "") )
          )
          yield {

            "CAST(%s AS VARCHAR)  || ".format(name)

          }
            ).mkString.dropRight(3)
        )

      rowList.clear()

      for( name:String <- columnNames.filter(
        columnName => ( (row(columnName).getOrElse("")  != "")   )
      )
      )      {

        rowList+=   ((name,row(name).get))

      }
      //Not Sure if to String will always match Varchar cast
      val filter:String =
        (for (column <- rowList)  yield " %s = %s AND ".format(column._1,column._2.toString)).mkString.dropRight(4)

      val md5Query =
        if (targetDefaultSchemaName.isEmpty)
          "UPDATE %s SET %s = %s WHERE %s".format(targetDefaultTableName,md5ColumnName,md5Calculator,filter)
        else
          "UPDATE %s.%s SET %s = %s WHERE %s".format(targetDefaultSchemaName.get,md5TargetTableName,md5ColumnName,md5Calculator,filter)

      processQuery = md5Query

      try {

        md5Backend.execute(processQuery)

        md5Backend.commit()

      }
      catch {

        case e:java.sql.SQLException  =>    {

          close

          println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

          throw e
        }

      }

    }


    md5NotNull

  }
  
  def md5Null:  Boolean  = {

    processQuery =
      if  (targetDefaultSchemaName.isEmpty)
        "select count(1) as count from %s where %s is not null".format(md5TargetTableName,md5ColumnName)
      else
        "select count(1) as count from %s.%s where %s is not null".format(targetDefaultSchemaName.get,md5TargetTableName,md5ColumnName)

    try {

      val result =  targetBackend.executeQuery(processQuery)

      result.next()

      return  (   (result.getInt("count"))    ==      0 )
    }
    catch {

      case e:java.sql.SQLException  =>    {

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
      }

    }

  }

  def md5NotNull:  Boolean  = {

    processQuery =
      if  (targetDefaultSchemaName.isEmpty)
        "select count(1) as count from %s where %s is null".format(md5TargetTableName,md5ColumnName)
      else
        "select count(1) as count from %s.%s where %s is null".format(targetDefaultSchemaName.get,md5TargetTableName,md5ColumnName)

    try {

      val result =  targetBackend.executeQuery(processQuery)

      result.next()

      return  (   (result.getInt("count"))    ==      0 )
    }
    catch {

      case e:java.sql.SQLException  =>    {

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
      }

    }

  }

  def md5ToNull:  Boolean = {

    processQuery  =
      if  (targetDefaultSchemaName.isEmpty)
        "UPDATE %s SET %s = null".format(md5TargetTableName,md5ColumnName)
      else
        "UPDATE %s.%s SET %s = null".format(targetDefaultSchemaName.get,md5TargetTableName,md5ColumnName)

    try {

      targetBackend.execute(processQuery)

      targetBackend.commit()

    }
    catch {

      case e:java.sql.SQLException  =>    {

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
      }

    }

    md5Null

  }

  def md5UniqueIndex(operation: String):  Boolean = {

    val indexUniqueId   =  (new Random(( (new Random()).nextInt(1000) ) )).nextInt(1000).toString

    val indexPrefix     =   "md5_unique_idx_"

    val indexName       =

      if (md5TargetTableName.indexOf("_") > 0 ) {

        indexPrefix.concat( md5TargetTableName.take(md5TargetTableName.indexOf("_")).concat("_").concat(indexUniqueId) )

      }
      else    {   //Assuming that the name is shorter then if it has no under scores

        indexPrefix.concat( md5TargetTableName.concat("_").concat(indexUniqueId) )

      }

    if (operation == "add"){

      processQuery =
        if  (targetDefaultSchemaName.isEmpty) {

          """
          create unique index %s on %s using btree (%s)
          """.format(indexName,md5TargetTableName,md5ColumnName)

        }
        else  {

          """
          create unique index %s on %s.%s using btree (%s)
          """.format(indexName,targetDefaultSchemaName.get ,md5TargetTableName,md5ColumnName)

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
		    c.relname     like    '%s%s'
		    order by
		    n.nspname,t.relname,c.relname
        """.format(targetDefaultSchemaName.get,  md5TargetTableName,indexPrefix,"%")

      val result  = targetBackend.executeQuery(processQuery)

      val indexes:MutableList[String]   =   MutableList.empty

      while (result.next()) {   indexes+= result.getString("index_name")  }

      indexes.foreach { index   =>

        processQuery   =
          """
          drop index if exists %s.%s restrict
          """.format(targetDefaultSchemaName.get,index)

        targetBackend.execute(processQuery)

      }

      targetBackend.commit()

      return true

    }
    else {



      println("Bad Operation.  Operation must be add or drop")

      return false
    }


  }

  def md5UniqueConstraint(operation:String): Boolean  = {

    val constraintName         =     "md5_unique_01"

    if (operation == "add"){

      processQuery =
        if  (targetDefaultSchemaName.isEmpty) {

          """
          alter table %s add constraint %s unique (%s)
          """.format(md5TargetTableName,constraintName,md5ColumnName)

        }
        else  {

          """
          alter table %s.%s add constraint %s unique (%s)
          """.format(targetDefaultSchemaName.get ,md5TargetTableName,constraintName,md5ColumnName)

        }


      targetBackend.execute(processQuery)

      targetBackend.commit()

      return true

    }

    else  if (operation == "drop")  {

      processQuery =
        if (targetDefaultSchemaName.isEmpty)  {

          """
          alter table %s drop constraint %s
          """.format(md5TargetTableName,constraintName)

        }
        else  {

          """
          alter table %s.%s drop constraint %s
          """.format(targetDefaultSchemaName.get,  md5TargetTableName,constraintName)

        }


      targetBackend.execute(processQuery)

      targetBackend.commit()

      return true

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }



  }

  def md5PerformanceIndex(operation: String):  Boolean = {
    //Performance index for adding md5 to concept_aggregate table
    //At present this needs to be implemented in the child classes

    if (operation == "add")   {

      false

    }
    else  if (operation == "drop")  {

      false

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  def performanceIndex(operation: String):  Boolean = {
    //Performance index for adding md5 to concept_aggregate table
    //At present this needs to be implemented in the child classes

    if (operation == "add")   {

      false

    }
    else  if (operation == "drop")  {

      false

    }
    else {

      println("Bad Operation.  Operation must be add or drop")

      return false

    }

  }

  def columns(tableName:String  = targetDefaultTableName): scala.collection.Map[String,DataType]    =   {
    //Get column name and data types on a target Table
    val columns:scala.collection.mutable.Map[String,DataType]       =     scala.collection.mutable.Map.empty

    if (targetDefaultSchemaName.isEmpty)  {

      processQuery   = "SELECT * FROM %s limit 1".format(tableName)

    }
    else  {

      processQuery   = "SELECT * FROM %s.%s limit 1".format(targetDefaultSchemaName.get,tableName)

    }

    try {

      val target  = DataTable(targetBackend,processQuery)

      var i = 0
      for(name <- target.column_names)   {

        columns(name)  = target.dataTypes(i)
        i+=1

      }


    }
    catch {

      case e:java.sql.SQLException  =>    {

        close

        println(e.getMessage + "\n" + e.getCause + "\n" + e.getSQLState + "\n")

        throw e
      }

    }

    columns.toMap

  }

  override def close:  Boolean = {

    log.close

    md5Backend.close

    sourceBackend.close

    targetBackend.close

    true
  }






}

