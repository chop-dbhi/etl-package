package edu.chop.cbmi.etl.library.transformers.aggregators

import edu.chop.cbmi.dataExpress.dataModels.{DataType, DataRow, DataTable}
import edu.chop.cbmi.dataExpress.backends.SqlBackend

import edu.chop.cbmi.dataExpress.dataModels.RichOption._
/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 3/7/12
 * Time: 5:43 PM
 * To change this template use File | Settings | File Templates.
 */


//row to row aggregator for the clarity concept table using fold left

case class aggregatorB[T](sourceDataTable: DataTable[T] , targetTableName:String, targetSchemaName: Option[String], targetBackend: SqlBackend )   {

  var pat_enc_csn_id:  String                       =   null

  var concept_id:  String                           =   null

  var extract_date: String                          =   null

  var md5: String                                   =   null

  var newAggregator                                 =   true

  val defaultLineNumber                             =   1

  val columnNames                                   =   sourceDataTable.column_names.+:("id")

  var aggregationContainer: java.lang.StringBuffer  =   new StringBuffer()

  var id                                            =   1
  
  def aggregate():Boolean   = {

    (new StringBuffer() /: sourceDataTable)   {

      (aggregation, rowToAggregate)  =>

        if (newAggregator)      {

        pat_enc_csn_id  = rowToAggregate.pat_enc_csn_id.asu[String]
        
        concept_id      = rowToAggregate.concept_id.asu[String]

        newAggregator   = false

        }

        if  (
              (rowToAggregate.pat_enc_csn_id.asu[String]  == pat_enc_csn_id )    &&
              (rowToAggregate.concept_id.asu[String]      == concept_id )
            )   {
         
          
          /*
          if (rowToAggregate.concept_value.as[String]  != None ) {
            //Append to the value to the existing aggregation
            aggregation.append(rowToAggregate.concept_value.asu[String])

          }
          */

          rowToAggregate.concept_value  match {

            case  Some(s)   =>  aggregation.append(s)

          }  
          
          
          /*
          
          if (rowToAggregate.extract_date.as[String]  != None ) {

            extract_date  =   rowToAggregate.extract_date.asu[String]

          }
          */
          
          //Want to use the date from the last line number
          extract_date  = { rowToAggregate.extract_date
                              match  {

                                case  Some(s)   => s.toString
                                case  _         => extract_date

                              }
          }

          aggregationContainer    =   aggregation

          aggregation

        }
        else  { //  Aggregator Id's have changed

          insertAggregation(aggregationContainer)

          //Update the Id's to those of the new row
          pat_enc_csn_id  = rowToAggregate.pat_enc_csn_id.asu[String]

          concept_id      = rowToAggregate.concept_id.asu[String]
          //Want to use the date from the last line number
          extract_date  = { rowToAggregate.extract_date
                              match  {

                                case  Some(s)   => s.toString
                                case  _         => extract_date

                              }
          }
          //Clear out the old aggregation
          aggregation.delete(0, aggregation.length())
          //Append the value from the new row to the fresh aggregation
          rowToAggregate.concept_value  match {

            case  Some(s)   =>  aggregation.append(s)

          }

          aggregationContainer    =   aggregation
          //End this section and the fold left will use this as the next input

          id += 1

          aggregation

        }



    }

    //Once the fold left is done, insert the very last aggregation

    insertAggregation(aggregationContainer)

    true

  }



  def insertAggregation(aggregation: StringBuffer):Boolean  =   {

    //  Create a row with the completed aggregation using previous id's
    //  Setting all md5's to null
    val rowToInsert         =   List  ( (id),
                                        (pat_enc_csn_id ),
                                        (concept_id),
                                        (defaultLineNumber),
                                        (aggregation.toString),
                                        (extract_date),
                                        (md5)
                                )

    //Create a datable that is bulk insert-able using the dataRow
    val rowToBulkInsert       =   DataTable(columnNames,rowToInsert)
    //Add the prepared row to the bulk insert batch
    targetBackend.batchInsert(targetTableName,rowToBulkInsert,targetSchemaName)

    true
 }


}